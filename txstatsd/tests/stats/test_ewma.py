# Copyright (C) 2011-2012 Canonical Services Ltd
#
# Permission is hereby granted, free of charge, to any person obtaining
# a copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject to
# the following conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
# IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
# CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
# TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
# SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

import math
from unittest import TestCase

from txstatsd.stats.ewma import Ewma


def mark_minutes(minutes, ewma):
    for i in range(1, minutes * 60, 5):
        ewma.tick()

class TestEwmaOneMinute(TestCase):
    def setUp(self):
        self.ewma = Ewma.one_minute_ewma()
        self.ewma.update(3)
        self.ewma.tick()

    def test_first_tick(self):
        self.assertTrue(
            (math.fabs(self.ewma.rate - 0.6) < 0.000001),
            'Should have a rate of 0.6 events/sec after the first tick')

    def test_one_minute(self):
        mark_minutes(1, self.ewma)
        self.assertTrue(
            (math.fabs(self.ewma.rate - 0.22072766) < 0.00000001),
            'Should have a rate of 0.22072766 events/sec after 1 minute')

    def test_two_minutes(self):
        mark_minutes(2, self.ewma)
        self.assertTrue(
            (math.fabs(self.ewma.rate - 0.08120117) < 0.00000001),
            'Should have a rate of 0.08120117 events/sec after 2 minutes')

    def test_three_minutes(self):
        mark_minutes(3, self.ewma)
        self.assertTrue(
            (math.fabs(self.ewma.rate - 0.02987224) < 0.00000001),
            'Should have a rate of 0.02987224 events/sec after 3 minutes')

    def test_four_minutes(self):
        mark_minutes(4, self.ewma)
        self.assertTrue(
            (math.fabs(self.ewma.rate - 0.01098938) < 0.00000001),
            'Should have a rate of 0.01098938 events/sec after 4 minutes')

    def test_five_minutes(self):
        mark_minutes(5, self.ewma)
        self.assertTrue(
            (math.fabs(self.ewma.rate - 0.00404277) < 0.00000001),
            'Should have a rate of 0.00404277 events/sec after 5 minutes')

    def test_six_minutes(self):
        mark_minutes(6, self.ewma)
        self.assertTrue(
            (math.fabs(self.ewma.rate - 0.00148725) < 0.00000001),
            'Should have a rate of 0.00148725 events/sec after 6 minutes')

    def test_seven_minutes(self):
        mark_minutes(7, self.ewma)
        self.assertTrue(
            (math.fabs(self.ewma.rate - 0.00054713) < 0.00000001),
            'Should have a rate of 0.00054713 events/sec after 7 minutes')

    def test_eight_minutes(self):
        mark_minutes(8, self.ewma)
        self.assertTrue(
            (math.fabs(self.ewma.rate - 0.00020128) < 0.00000001),
            'Should have a rate of 0.00020128 events/sec after 8 minutes')

    def test_nine_minutes(self):
        mark_minutes(9, self.ewma)
        self.assertTrue(
            (math.fabs(self.ewma.rate - 0.00007405) < 0.00000001),
            'Should have a rate of 0.00007405 events/sec after 9 minutes')

    def test_ten_minutes(self):
        mark_minutes(10, self.ewma)
        self.assertTrue(
            (math.fabs(self.ewma.rate - 0.00002724) < 0.00000001),
            'Should have a rate of 0.00002724 events/sec after 10 minutes')

    def test_eleven_minutes(self):
        mark_minutes(11, self.ewma)
        self.assertTrue(
            (math.fabs(self.ewma.rate - 0.00001002) < 0.00000001),
            'Should have a rate of 0.00001002 events/sec after 11 minutes')

    def test_twelve_minutes(self):
        mark_minutes(12, self.ewma)
        self.assertTrue(
            (math.fabs(self.ewma.rate - 0.00000369) < 0.00000001),
            'Should have a rate of 0.00000369 events/sec after 12 minutes')

    def test_thirteen_minutes(self):
        mark_minutes(13, self.ewma)
        self.assertTrue(
            (math.fabs(self.ewma.rate - 0.00000136) < 0.00000001),
            'Should have a rate of 0.00000136 events/sec after 13 minutes')

    def test_fourteen_minutes(self):
        mark_minutes(14, self.ewma)
        self.assertTrue(
            (math.fabs(self.ewma.rate - 0.00000050) < 0.00000001),
            'Should have a rate of 0.00000050 events/sec after 14 minutes')

    def test_fifteen_minutes(self):
        mark_minutes(15, self.ewma)
        self.assertTrue(
            (math.fabs(self.ewma.rate - 0.00000018) < 0.00000001),
            'Should have a rate of 0.00000018 events/sec after 15 minutes')


class TestEwmaFiveMinute(TestCase):
    def setUp(self):
        self.ewma = Ewma.five_minute_ewma()
        self.ewma.update(3)
        self.ewma.tick()

    def test_first_tick(self):
        self.assertTrue(
            (math.fabs(self.ewma.rate - 0.6) < 0.000001),
            'Should have a rate of 0.6 events/sec after the first tick')

    def test_one_minute(self):
        mark_minutes(1, self.ewma)
        self.assertTrue(
            (math.fabs(self.ewma.rate - 0.49123845) < 0.00000001),
            'Should have a rate of 0.49123845 events/sec after 1 minute')

    def test_two_minutes(self):
        mark_minutes(2, self.ewma)
        self.assertTrue(
            (math.fabs(self.ewma.rate - 0.40219203) < 0.00000001),
            'Should have a rate of 0.40219203 events/sec after 2 minutes')

    def test_three_minutes(self):
        mark_minutes(3, self.ewma)
        self.assertTrue(
            (math.fabs(self.ewma.rate - 0.32928698) < 0.00000001),
            'Should have a rate of 0.32928698 events/sec after 3 minutes')

    def test_four_minutes(self):
        mark_minutes(4, self.ewma)
        self.assertTrue(
            (math.fabs(self.ewma.rate - 0.26959738) < 0.00000001),
            'Should have a rate of 0.26959738 events/sec after 4 minutes')

    def test_five_minutes(self):
        mark_minutes(5, self.ewma)
        self.assertTrue(
            (math.fabs(self.ewma.rate - 0.22072766) < 0.00000001),
            'Should have a rate of 0.22072766 events/sec after 5 minutes')

    def test_six_minutes(self):
        mark_minutes(6, self.ewma)
        self.assertTrue(
            (math.fabs(self.ewma.rate - 0.18071653) < 0.00000001),
            'Should have a rate of 0.18071653 events/sec after 6 minutes')

    def test_seven_minutes(self):
        mark_minutes(7, self.ewma)
        self.assertTrue(
            (math.fabs(self.ewma.rate - 0.14795818) < 0.00000001),
            'Should have a rate of 0.14795818 events/sec after 7 minutes')

    def test_eight_minutes(self):
        mark_minutes(8, self.ewma)
        self.assertTrue(
            (math.fabs(self.ewma.rate - 0.12113791) < 0.00000001),
            'Should have a rate of 0.12113791 events/sec after 8 minutes')

    def test_nine_minutes(self):
        mark_minutes(9, self.ewma)
        self.assertTrue(
            (math.fabs(self.ewma.rate - 0.09917933) < 0.00000001),
            'Should have a rate of 0.09917933 events/sec after 9 minutes')

    def test_ten_minutes(self):
        mark_minutes(10, self.ewma)
        self.assertTrue(
            (math.fabs(self.ewma.rate - 0.08120117) < 0.00000001),
            'Should have a rate of 0.08120117 events/sec after 10 minutes')

    def test_eleven_minutes(self):
        mark_minutes(11, self.ewma)
        self.assertTrue(
            (math.fabs(self.ewma.rate - 0.06648190) < 0.00000001),
            'Should have a rate of 0.06648190 events/sec after 11 minutes')

    def test_twelve_minutes(self):
        mark_minutes(12, self.ewma)
        self.assertTrue(
            (math.fabs(self.ewma.rate - 0.05443077) < 0.00000001),
            'Should have a rate of 0.05443077 events/sec after 12 minutes')

    def test_thirteen_minutes(self):
        mark_minutes(13, self.ewma)
        self.assertTrue(
            (math.fabs(self.ewma.rate - 0.04456415) < 0.00000001),
            'Should have a rate of 0.04456415 events/sec after 13 minutes')

    def test_fourteen_minutes(self):
        mark_minutes(14, self.ewma)
        self.assertTrue(
            (math.fabs(self.ewma.rate - 0.03648604) < 0.00000001),
            'Should have a rate of 0.03648604 events/sec after 14 minutes')

    def test_fifteen_minutes(self):
        mark_minutes(15, self.ewma)
        self.assertTrue(
            (math.fabs(self.ewma.rate - 0.02987224) < 0.00000001),
            'Should have a rate of 0.02987224 events/sec after 15 minutes')


class TestEwmaFifteenMinute(TestCase):
    def setUp(self):
        self.ewma = Ewma.fifteen_minute_ewma()
        self.ewma.update(3)
        self.ewma.tick()

    def test_first_tick(self):
        self.assertTrue(
            (math.fabs(self.ewma.rate - 0.6) < 0.000001),
            'Should have a rate of 0.6 events/sec after the first tick')

    def test_one_minute(self):
        mark_minutes(1, self.ewma)
        self.assertTrue(
            (math.fabs(self.ewma.rate - 0.56130419) < 0.00000001),
            'Should have a rate of 0.56130419 events/sec after 1 minute')

    def test_two_minutes(self):
        mark_minutes(2, self.ewma)
        self.assertTrue(
            (math.fabs(self.ewma.rate - 0.52510399) < 0.00000001),
            'Should have a rate of 0.52510399 events/sec after 2 minutes')

    def test_three_minutes(self):
        mark_minutes(3, self.ewma)
        self.assertTrue(
            (math.fabs(self.ewma.rate - 0.49123845) < 0.00000001),
            'Should have a rate of 0.49123845 events/sec after 3 minutes')

    def test_four_minutes(self):
        mark_minutes(4, self.ewma)
        self.assertTrue(
            (math.fabs(self.ewma.rate - 0.45955700) < 0.00000001),
            'Should have a rate of 0.45955700 events/sec after 4 minutes')

    def test_five_minutes(self):
        mark_minutes(5, self.ewma)
        self.assertTrue(
            (math.fabs(self.ewma.rate - 0.42991879) < 0.00000001),
            'Should have a rate of 0.42991879 events/sec after 5 minutes')

    def test_six_minutes(self):
        mark_minutes(6, self.ewma)
        self.assertTrue(
            (math.fabs(self.ewma.rate - 0.40219203) < 0.00000001),
            'Should have a rate of 0.40219203 events/sec after 6 minutes')

    def test_seven_minutes(self):
        mark_minutes(7, self.ewma)
        self.assertTrue(
            (math.fabs(self.ewma.rate - 0.37625345) < 0.00000001),
            'Should have a rate of 0.37625345 events/sec after 7 minutes')

    def test_eight_minutes(self):
        mark_minutes(8, self.ewma)
        self.assertTrue(
            (math.fabs(self.ewma.rate - 0.35198773) < 0.00000001),
            'Should have a rate of 0.35198773 events/sec after 8 minutes')

    def test_nine_minutes(self):
        mark_minutes(9, self.ewma)
        self.assertTrue(
            (math.fabs(self.ewma.rate - 0.32928698) < 0.00000001),
            'Should have a rate of 0.32928698 events/sec after 9 minutes')

    def test_ten_minutes(self):
        mark_minutes(10, self.ewma)
        self.assertTrue(
            (math.fabs(self.ewma.rate - 0.30805027) < 0.00000001),
            'Should have a rate of 0.30805027 events/sec after 10 minutes')

    def test_eleven_minutes(self):
        mark_minutes(11, self.ewma)
        self.assertTrue(
            (math.fabs(self.ewma.rate - 0.28818318) < 0.00000001),
            'Should have a rate of 0.28818318 events/sec after 11 minutes')

    def test_twelve_minutes(self):
        mark_minutes(12, self.ewma)
        self.assertTrue(
            (math.fabs(self.ewma.rate - 0.26959738) < 0.00000001),
            'Should have a rate of 0.26959738 events/sec after 12 minutes')

    def test_thirteen_minutes(self):
        mark_minutes(13, self.ewma)
        self.assertTrue(
            (math.fabs(self.ewma.rate - 0.25221023) < 0.00000001),
            'Should have a rate of 0.25221023 events/sec after 13 minutes')

    def test_fourteen_minutes(self):
        mark_minutes(14, self.ewma)
        self.assertTrue(
            (math.fabs(self.ewma.rate - 0.23594443) < 0.00000001),
            'Should have a rate of 0.23594443 events/sec after 14 minutes')

    def test_fifteen_minutes(self):
        mark_minutes(15, self.ewma)
        self.assertTrue(
            (math.fabs(self.ewma.rate - 0.22072766) < 0.00000001),
            'Should have a rate of 0.22072766 events/sec after 15 minutes')
