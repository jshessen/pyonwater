"""Tests for pyonwater meter reader."""  # nosec: B101, B106

import datetime
import json
import logging
from typing import Any
from unittest.mock import patch

from aiohttp import web
from conftest import (
    build_client,
    change_units_decorator,
    mock_historical_data_endpoint,
    mock_historical_data_no_data_endpoint,
    mock_read_meter_endpoint,
    mock_signin_endpoint,
)
import pytest
from pydantic import ValidationError

from pyonwater import EyeOnWaterAPIError, EyeOnWaterResponseIsEmpty, MeterReader
from pyonwater.models import HistoricalData


@pytest.mark.asyncio()
async def test_meter_reader(aiohttp_client: Any) -> None:
    """Basic meter reader test."""
    app = web.Application()

    app.router.add_post("/account/signin", mock_signin_endpoint)
    app.router.add_post("/api/2/residential/new_search", mock_read_meter_endpoint)
    app.router.add_post("/api/2/residential/consumption", mock_historical_data_endpoint)

    websession = await aiohttp_client(app)

    _, client = await build_client(websession)

    meter_reader = MeterReader(meter_uuid="meter_uuid", meter_id="meter_id")

    meter_info = await meter_reader.read_meter_info(client=client)
    assert meter_info.reading.latest_read.full_read != 0  # nosec: B101

    await meter_reader.read_historical_data(client=client, days_to_load=1)


@pytest.mark.asyncio()
async def test_meter_reader_nodata(aiohttp_client: Any) -> None:
    """Basic meter reader test."""
    app = web.Application()

    app.router.add_post("/account/signin", mock_signin_endpoint)
    app.router.add_post("/api/2/residential/new_search", mock_read_meter_endpoint)
    app.router.add_post(
        "/api/2/residential/consumption",
        mock_historical_data_no_data_endpoint,
    )

    websession = await aiohttp_client(app)

    _, client = await build_client(websession)

    meter_reader = MeterReader(meter_uuid="meter_uuid", meter_id="meter_id")

    meter_info = await meter_reader.read_meter_info(client=client)
    assert meter_info.reading.latest_read.full_read != 0  # nosec: B101

    data = await meter_reader.read_historical_data(client=client, days_to_load=1)
    assert data == []  # nosec: B101


@pytest.mark.asyncio()
async def test_meter_reader_wrong_units(aiohttp_client: Any) -> None:
    """Test reading date with unknown units."""
    app = web.Application()

    app.router.add_post("/account/signin", mock_signin_endpoint)
    app.router.add_post(
        "/api/2/residential/new_search",
        change_units_decorator(mock_read_meter_endpoint, "hey"),
    )

    websession = await aiohttp_client(app)

    _, client = await build_client(websession)

    meter_reader = MeterReader(meter_uuid="meter_uuid", meter_id="meter_id")

    with pytest.raises(EyeOnWaterAPIError):
        await meter_reader.read_meter_info(client=client)


@pytest.mark.asyncio()
async def test_meter_reader_empty_response(aiohttp_client: Any) -> None:
    """Test handling of empty API responses.

    Real API behavior when params are invalid.
    """
    app = web.Application()

    app.router.add_post("/account/signin", mock_signin_endpoint)
    app.router.add_post("/api/2/residential/new_search", mock_read_meter_endpoint)

    async def mock_empty_response(_request: web.Request) -> web.Response:
        """Mock endpoint that returns empty response like real API does.

        Simulates real API behavior with invalid params.
        """
        return web.Response(text="")

    app.router.add_post("/api/2/residential/consumption", mock_empty_response)

    websession = await aiohttp_client(app)

    _, client = await build_client(websession)

    meter_reader = MeterReader(meter_uuid="meter_uuid", meter_id="meter_id")

    # Empty responses should be handled gracefully (not crash)
    # The read_historical_data method catches EyeOnWaterResponseIsEmpty and continues
    data = await meter_reader.read_historical_data(client=client, days_to_load=1)
    assert data == []  # nosec: B101  # Empty response results in no data points


@pytest.mark.asyncio()
async def test_meter_reader_raises_for_multiple_meters(aiohttp_client: Any) -> None:
    """Verify EyeOnWaterAPIError raised when new_search returns multiple hits."""
    with open("tests/mock_data/read_meter_mock_anonymized.json", encoding="utf-8") as f:
        single_hit_data = json.load(f)

    hit = single_hit_data["elastic_results"]["hits"]["hits"][0]
    single_hit_data["elastic_results"]["hits"]["hits"] = [hit, hit]

    async def mock_two_meters(_request: web.Request) -> web.Response:
        return web.Response(text=json.dumps(single_hit_data))

    app = web.Application()
    app.router.add_post("/account/signin", mock_signin_endpoint)
    app.router.add_post("/api/2/residential/new_search", mock_two_meters)

    websession = await aiohttp_client(app)
    _, client = await build_client(websession)
    meter_reader = MeterReader(meter_uuid="meter_uuid", meter_id="meter_id")

    with pytest.raises(EyeOnWaterAPIError, match="More than one meter reading found"):
        await meter_reader.read_meter_info(client=client)


@pytest.mark.asyncio()
async def test_meter_reader_historical_debug_logging(aiohttp_client: Any) -> None:
    """With DEBUG logging enabled, the raw-response branch executes."""
    app = web.Application()
    app.router.add_post("/account/signin", mock_signin_endpoint)
    app.router.add_post("/api/2/residential/new_search", mock_read_meter_endpoint)
    app.router.add_post("/api/2/residential/consumption", mock_historical_data_endpoint)

    websession = await aiohttp_client(app)
    _, client = await build_client(websession)
    reader = MeterReader(meter_uuid="meter_uuid", meter_id="meter_id")

    logger = logging.getLogger("pyonwater.meter_reader")
    original_level = logger.level
    try:
        logger.setLevel(logging.DEBUG)
        data = await reader.read_historical_data(client=client, days_to_load=1)
    finally:
        logger.setLevel(original_level)

    assert len(data) > 0  # nosec: B101


@pytest.mark.asyncio()
async def test_meter_reader_historical_invalid_json_raises(aiohttp_client: Any) -> None:
    """Verify EyeOnWaterAPIError raised when consumption returns malformed JSON."""

    async def mock_bad_json(_request: web.Request) -> web.Response:
        return web.Response(text="{this is not: valid json!!!}")

    app = web.Application()
    app.router.add_post("/account/signin", mock_signin_endpoint)
    app.router.add_post("/api/2/residential/new_search", mock_read_meter_endpoint)
    app.router.add_post("/api/2/residential/consumption", mock_bad_json)

    websession = await aiohttp_client(app)
    _, client = await build_client(websession)
    reader = MeterReader(meter_uuid="meter_uuid", meter_id="meter_id")

    with pytest.raises(EyeOnWaterAPIError):  # nosec: B101
        await reader.read_historical_data(client=client, days_to_load=1)


@pytest.mark.asyncio()
async def test_meter_reader_historical_invalid_json_debug_logging(
    aiohttp_client: Any,
) -> None:
    """With DEBUG logging enabled, invalid JSON logs the raw response before raising."""

    async def mock_bad_json(_request: web.Request) -> web.Response:
        return web.Response(text="{this is not: valid json!!!}")

    app = web.Application()
    app.router.add_post("/account/signin", mock_signin_endpoint)
    app.router.add_post("/api/2/residential/new_search", mock_read_meter_endpoint)
    app.router.add_post("/api/2/residential/consumption", mock_bad_json)

    websession = await aiohttp_client(app)
    _, client = await build_client(websession)
    reader = MeterReader(meter_uuid="meter_uuid", meter_id="meter_id")

    logger = logging.getLogger("pyonwater.meter_reader")
    original_level = logger.level
    try:
        logger.setLevel(logging.DEBUG)
        with pytest.raises(EyeOnWaterAPIError):  # nosec: B101
            await reader.read_historical_data(client=client, days_to_load=1)
    finally:
        logger.setLevel(original_level)


@pytest.mark.asyncio()
async def test_meter_reader_historical_validation_error_empty_input_raises_empty(
    aiohttp_client: Any,
) -> None:
    """EyeOnWaterResponseIsEmpty raised when ValidationError has json_invalid + falsy input.

    Covers the second empty-body safety net in meter_reader.py (lines 255-260):
    a raw response that passes the stripped-string check but whose pydantic parse
    produces a ``json_invalid`` ValidationError with an empty ``input`` field.

    The trigger is constructed by capturing a real ValidationError from an
    empty-string parse (``HistoricalData.model_validate_json("")`` → type
    ``json_invalid``, input ``""``) and patching the call site so it fires
    even when raw_data is non-empty.
    """
    # Build a real pydantic ValidationError with type="json_invalid" and
    # input="" (falsy) by parsing an empty string.
    falsy_input_error: ValidationError | None = None
    try:
        HistoricalData.model_validate_json("")
    except ValidationError as empty_json_error:
        falsy_input_error = empty_json_error
    else:
        pytest.fail("Expected ValidationError from empty-string parse")  # nosec: B101

    assert falsy_input_error is not None  # guaranteed by else branch above

    async def mock_non_empty_response(_request: web.Request) -> web.Response:
        # Non-empty, non-null body so the stripped-string check passes;
        # the ValidationError patch below is the only way to reach lines 255-260.
        return web.Response(text="{non_empty_but_will_be_intercepted}")

    app = web.Application()
    app.router.add_post("/account/signin", mock_signin_endpoint)
    app.router.add_post("/api/2/residential/new_search", mock_read_meter_endpoint)
    app.router.add_post("/api/2/residential/consumption", mock_non_empty_response)

    websession = await aiohttp_client(app)
    _, client = await build_client(websession)
    reader = MeterReader(meter_uuid="meter_uuid", meter_id="meter_id")

    with patch.object(
        HistoricalData,
        "model_validate_json",
        side_effect=falsy_input_error,
    ):
        # Call the single-day method directly: the outer read_historical_data
        # loop catches EyeOnWaterResponseIsEmpty and continues, so we bypass it
        # to assert the exception propagates from the per-day fetch.
        with pytest.raises(EyeOnWaterResponseIsEmpty):  # nosec: B101
            await reader.read_historical_data_one_day(
                client=client,
                date=datetime.datetime(2024, 1, 15),
            )
