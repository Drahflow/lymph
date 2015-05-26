import unittest
from mock import patch

import time

from lymph.core.circuitbreaker import MultiCauseCircuitBreaker
from lymph.exceptions import CircuitBreakerOpen

class CircuitBreakerTest(unittest.TestCase):
    def test_breaking(self):
        breaker = MultiCauseCircuitBreaker()
        breaker.add_method('A', 'a')

        for i in range(10):
            breaker.observe_failure('A', 'a')

        self.assertRaises(CircuitBreakerOpen, breaker.select_endpoint, 'a')

    def test_closing(self):
        breaker = MultiCauseCircuitBreaker()
        breaker.add_method('A', 'a')

        for i in range(10):
            breaker.observe_failure('A', 'a')
        breaker.observe_success('A', 'a')

        self.assertEqual(breaker.select_endpoint('a'), 'A')

    def test_remains_closed(self):
        breaker = MultiCauseCircuitBreaker()
        breaker.add_method('A', 'a')

        for i in range(5):
            breaker.observe_failure('A', 'a')

        self.assertEqual(breaker.select_endpoint('a'), 'A')

    def test_breaking_endpoint(self):
        breaker = MultiCauseCircuitBreaker()
        breaker.add_method('A', 'a1')
        breaker.add_method('A', 'a2')
        breaker.add_method('B', 'b')

        for i in range(5):
            breaker.observe_failure('A', 'a1')
            breaker.observe_failure('A', 'a2')
            breaker.observe_success('B', 'b')

        self.assertEqual(breaker.select_endpoint('b'), 'B')
        self.assertRaises(CircuitBreakerOpen, breaker.select_endpoint, 'a1')

        now = time.monotonic()
        with patch('time.monotonic', new=lambda: now + 120):
            self.assertEqual(breaker.select_endpoint('a1'), 'A')

    def test_breaking_method(self):
        breaker = MultiCauseCircuitBreaker()
        breaker.add_method('A', '1')
        breaker.add_method('A', '2')
        breaker.add_method('B', '1')

        for i in range(5):
            breaker.observe_failure('A', '1')
            breaker.observe_failure('B', '1')
            breaker.observe_success('A', '2')

        self.assertEqual(breaker.select_endpoint('2'), 'A')
        self.assertRaises(CircuitBreakerOpen, breaker.select_endpoint, '1')

        now = time.monotonic()
        with patch('time.monotonic', new=lambda: now + 120):
            self.assertIn(breaker.select_endpoint('1'), ['A', 'B'])

    def test_breaking_global(self):
        breaker = MultiCauseCircuitBreaker()
        breaker.add_method('A', '1')
        breaker.add_method('A', '2')
        breaker.add_method('B', '1')
        breaker.add_method('B', '2')

        for i in range(2):
            breaker.observe_failure('A', '1')
            breaker.observe_failure('A', '2')
            breaker.observe_failure('B', '1')
            breaker.observe_failure('B', '2')

        self.assertRaises(CircuitBreakerOpen, breaker.select_endpoint, '1')

        now = time.monotonic()
        with patch('time.monotonic', new=lambda: now + 120):
            self.assertIn(breaker.select_endpoint('1'), ['A', 'B'])

    def test_closing_global(self):
        breaker = MultiCauseCircuitBreaker()
        breaker.add_method('A', '1')
        breaker.add_method('A', '2')
        breaker.add_method('B', '1')
        breaker.add_method('B', '2')
        breaker.add_method('C', '3')

        for i in range(4):
            breaker.observe_failure('A', '1')
            breaker.observe_failure('A', '2')
            breaker.observe_failure('B', '1')
            breaker.observe_failure('B', '2')
        breaker.observe_success('C', '3')

        self.assertEqual(breaker.select_endpoint('3'), 'C')
        self.assertRaises(CircuitBreakerOpen, breaker.select_endpoint, '1')
        self.assertRaises(CircuitBreakerOpen, breaker.select_endpoint, '2')

        now = time.monotonic()
        with patch('time.monotonic', new=lambda: now + 120):
            self.assertIn(breaker.select_endpoint('1'), ['A', 'B'])

    def test_cleanup(self):
        breaker = MultiCauseCircuitBreaker()
        breaker.add_method('A', '1')
        breaker.observe_failure('A', '1')

        now = time.monotonic()
        with patch('time.monotonic', new=lambda: now + 99999):
            self.assertEqual(breaker.select_endpoint('1'), 'A')
            self.assertFalse(breaker.endpoint_fail)
            self.assertFalse(breaker.method_fail)
            self.assertFalse(breaker.method_instance_fail)
