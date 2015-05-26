import logging
import time
import random

from lymph.exceptions import NotConnected, CircuitBreakerOpen

logger = logging.getLogger(__name__)

class MultiCauseCircuitBreaker:
    """
    A circuit breaker which inferes failure causes.

    It models five reasons why a request can fail.
    1.) Something is globally broken, say the network is overloaded - everything will fail
    2.) A service instance is down - all requests to this endpoint will fail
    3.) The implementation of a method is errornous - all requests using this method will fail
    4.) A specific method on a specific service is down - requests to this exact method will fail
    5.) Just a bad request - typically caused by invalid parameters

    Whenever a failure occurs, an error is counted for each of the four scenarios.
    Whenever a success occurs, all relevant error counts are reset.
    Since errors and successes occur for different endpoint/method-pairs, the most general
    scenario for which we have not seen a success will reach MAXIMUM_ERRORS_BEFORE_OPEN first
    and the circuit breaker will trigger for this scenario.

    When selecting endpoints, this code tries to select those with the lowest error count
    to maximize the chances of success. However, if all clients would select the same endpoint
    it would get overloaded. Hence instead of selecting just the best, this code assumes
    1/OVERPROVISIONING_FACTOR of the servers would be enough to handle all the load and distributes
    requests to the slice of this size which has the highest chance of success.

    == connections to a "real" probabilistic model ==

    A priori there is a certain (very low) probability that a specific scenario is currently the case.
    When a failure occurs, this can either be due to 5. or because of some persistent reason, i.e.
    it is weak evidence for all four scenarios of this specific endpoint/method-pair.

    When multiple failures occur, the probability that nothing of 1.-4. is the case (i.e. all is well)
    is multiplied by some factor below 1 depending on the (assumed) probability of 5..
    Instead of modelling floating-point probabilities, this code just counts errors which is equivalent
    to storing the (negative, and in a nonstandard-base) logarithm of the probability of the scenarios 1.-4..

    The MAXIMUM_ERRORS_BEFORE_OPEN is equivalent to the threshold when the a-priori probability of failure
    corrected by the accumulated evidence drops so low that we are convinced of a persistent failure.
    
    Assuming a constant rate of errors being fixed, the relevance of an observed failure weakens exponentially
    over time when calculating the probability of a current failure. Realistic values however would specify
    fixing times of hours, which would prevent retries over quite a long time. This also means that all
    observed failures can be handled equivalently without introducing too much error.
    """

    MAXIMUM_ERRORS_BEFORE_OPEN = 7
    COOLDOWN_SECONDS = 60
    OVERPROVISIONING_FACTOR = 2
    CLEANUP_INTERVAL = 3600

    def __init__(self, adjust_endpoint_rating=None):
        self.global_fail = [0, None]
        self.endpoint_fail = {}
        self.method_fail = {}
        self.method_instance_fail = {}
        self.adjust_endpoint_rating = adjust_endpoint_rating or self._adjust_endpoint_rating
        self.last_cleanup = time.monotonic()

    def add_method(self, endpoint, method):
        if not endpoint in self.endpoint_fail:
            self.endpoint_fail[endpoint] = [0, None]
        if not method in self.method_fail:
            self.method_fail[method] = [0, None]
            self.method_instance_fail[method] = {}
        if not endpoint in self.method_instance_fail[method]:
            self.method_instance_fail[method][endpoint] = [0, None]

    def observe_failure(self, endpoint, method):
        now = time.monotonic()

        self.global_fail[0] += 1
        self.global_fail[1] = now
        self.endpoint_fail[endpoint][0] += 1
        self.endpoint_fail[endpoint][1] = now
        self.method_fail[method][0] += 1
        self.method_fail[method][1] = now
        self.method_instance_fail[method][endpoint][0] += 1
        self.method_instance_fail[method][endpoint][1] = now

    def observe_success(self, endpoint, method):
        self.global_fail[0] = 0
        self.global_fail[1] = None
        self.endpoint_fail[endpoint][0] = 0
        self.endpoint_fail[endpoint][1] = None
        self.method_fail[method][0] = 0
        self.method_fail[method][1] = None
        self.method_instance_fail[method][endpoint][0] = 0
        self.method_instance_fail[method][endpoint][1] = None

    def _adjust_endpoint_rating(self, ratings):
        return ratings

    def select_endpoint(self, method, endpoints=None):
        now = time.monotonic()
        if self.last_cleanup < now - self.CLEANUP_INTERVAL:
            self._cleanup()

        ignore_before = now - self.COOLDOWN_SECONDS

        def apply_ignore_before(status):
            if status[1] <= ignore_before:
                return 0
            return status[0]

        if apply_ignore_before(self.global_fail) > self.MAXIMUM_ERRORS_BEFORE_OPEN:
            raise CircuitBreakerOpen("global failure rate too high")
        
        if method not in self.method_fail:
            raise NotConnected("no endpoints configured for method: %s" % method)

        if apply_ignore_before(self.method_fail[method]) > self.MAXIMUM_ERRORS_BEFORE_OPEN:
            raise CircuitBreakerOpen("method failure rate too high for method %s" % method)

        def sort_key(endpoint_key):
            return max(apply_ignore_before(self.endpoint_fail[endpoint_key]),
                    apply_ignore_before(self.method_instance_fail[method][endpoint_key]))

        if not endpoints:
            endpoints = self.method_instance_fail[method].keys()

        ratings = { endpoint: sort_key(endpoint) for endpoint in endpoints }
        ratings = self.adjust_endpoint_rating(ratings)

        if not ratings:
            raise NotConnected("no endpoints available for method: %s" % method)

        endpoints = sorted(ratings.keys(), key=ratings.get)
        endpoints = endpoints[0:1 + int((len(endpoints) - 1) / self.OVERPROVISIONING_FACTOR)]
        endpoint = random.choice(endpoints)

        if apply_ignore_before(self.endpoint_fail[endpoint]) > self.MAXIMUM_ERRORS_BEFORE_OPEN:
            raise CircuitBreakerOpen("endpoint failure rate too high for endpoint %s" % endpoint)
        if apply_ignore_before(self.method_instance_fail[method][endpoint]) > self.MAXIMUM_ERRORS_BEFORE_OPEN:
            raise CircuitBreakerOpen("method instance failure rate too high for (endpoint, method) = (%s, %s)" % (endpoint, method))

        return endpoint
    
    def _cleanup(self):
        cleanup_before = time.monotonic() - self.CLEANUP_INTERVAL

        for endpoint in self.endpoint_fail.keys():
            if (self.endpoint_fail[endpoint][1] or 0) < cleanup_before:
                self.endpoint_fail.pop(endpoint)
        for method in self.method_fail.keys():
            if (self.method_fail[method][1] or 0) < cleanup_before:
                self.method_fail.pop(method)
            for endpoint in self.method_instance_fail[method].keys():
                if (self.method_instance_fail[method][endpoint][1] or 0) < cleanup_before:
                    self.method_instance_fail[method].pop(endpoint)
                    if not self.method_instance_fail[method]:
                        self.method_instance_fail.pop(method)

        self.last_cleanup = time.monotonic()
