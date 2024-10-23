# fmt: off
# pylint: disable=abstract-method,unnecessary-dunder-call,expression-not-assigned

import pytest
from pytest import fixture, FixtureRequest, mark
from datetime import date
from modules.Filters import Filters
from modules.Names import Names

class TestFilters:
    """Unit tests for the Filters class"""

    # Note that test fixtures 'pkrd_fresh_row', 'pkrd_frozen_row', 'fresh_fresh_row', etc 
    # are defined in conftest.py
    @pytest.mark.parametrize(
        "row, types, depot_type, expected",
        [
            (
                'pkrd_fresh_row',
                ["PKRD"],
                "NFSI Fresh",
                True,
            ),
            (
                'pkrd_frozen_row',
                ["PKRD"],
                "NFSI Fresh",
                False,
            ),
            (
                'fresh_fresh_row',
                ["NFSI Fresh"],
                "NFSI Fresh",
                True,
            ),
            (
                'frozen_frozen_row',
                ["NFSI Fresh"],
                "NFSI Frozen",
                False,
            ),
        ],
    )
    def test_filter_by_types(self, row, types: list[str], depot_type: str, expected: bool, request: FixtureRequest):
        row = request.getfixturevalue(row)
        assert Filters.filter_by_types(row, types, depot_type) == expected

    @pytest.mark.parametrize(
        "row, category, expected",
        [
            (
                'pkrd_fresh_row',
                "NFSI Fresh",
                True,
            ),
            (
                'pkrd_frozen_row',
                "NFSI Fresh",
                False,
            ),
            (
                'frozen_frozen_row',
                "NFSI Frozen",
                True,
            ),
            (
                'fresh_fresh_row',
                "NFSI Frozen",
                False,
            ),
        ],
    )
    def test_filter_by_category(self, row, category: str, expected: bool, request: FixtureRequest):
        row = request.getfixturevalue(row)
        assert Filters.filter_by_category(row, category) == expected

    @pytest.mark.parametrize(
        "row, dates, expected",
        [
            (
                'pkrd_fresh_row',
                {
                    Names.START_DATE: date(2023, 1, 1),
                    Names.END_DATE: date(2023, 1, 31)
                },
                True,
            ),
            (
                'frozen_frozen_row',
                {
                    Names.START_DATE: date(2023, 1, 10),
                    Names.END_DATE: date(2023, 1, 12)
                },
                False,
            ),
            (
                'fresh_fresh_row',
                {
                    Names.START_DATE: date(2023, 12, 31),
                    Names.END_DATE: date(2024, 2, 14)
                },
                True,
            ),
            (
                'pkrd_frozen_row',
                {},
                True
            ),
            (
                'pkrd_frozen_row',
                {
                    Names.START_DATE: date(2023, 1, 15),
                    Names.END_DATE: date(2023, 1, 16)
                },
                False
            )            
        ],
    )
    def test_filter_for_dates(self, row, dates: dict, expected: bool, request: FixtureRequest):
        row = request.getfixturevalue(row)
        assert Filters.filter_for_dates(row, dates) == expected

    @pytest.mark.parametrize(
        "row, prefix, expected",
        [
            (
                'pkrd_fresh_row',
                "SS",
                True
            ),
            (
                'pkrd_frozen_row',
                "MM",
                False
            ),
            (
                'fresh_fresh_row',
                "ZZ",
                True
            ),
        ],
    )
    def test_filter_exclude_moveorder_prefix(self, row, prefix: str, expected: bool, request: FixtureRequest):
        row = request.getfixturevalue(row)
        assert Filters.filter_exclude_moveorder_prefix(row, prefix) == expected

    @pytest.mark.parametrize(
        "row, depot_id, expected",
        [
            (
                'pkrd_fresh_row',
                "CLS",
                True
            ),
            (
                'pkrd_frozen_row',
                "123",
                False
            ),
            (
                'fresh_fresh_row',
                "CRA",
                True
            ),
            (
                'frozen_frozen_row',
                "987",
                False
            ),
        ],
    )
    def test_filter_exclude_depot_id(self, row, depot_id: str, expected: bool, request: FixtureRequest):
        row = request.getfixturevalue(row)
        assert Filters.filter_exclude_depot_id(row, depot_id) == expected

# fmt: on