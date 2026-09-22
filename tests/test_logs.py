import asyncio

from q.logs import (
    append_log_line,
    history_paths,
    stream_log_lines,
)


def test_rotation_preserves_chronological_history(
    tmp_path,
):
    path = tmp_path / "q.ndjson"

    for value in (
        "AAAAAAAAAA\n",
        "BBBBBBBBBB\n",
        "CCCCCCCCCC\n",
    ):
        append_log_line(
            path,
            value,
            rotate_bytes=15,
            archives=2,
        )

    paths = history_paths(
        path,
        archives=2,
    )

    assert [
        item.name
        for item in paths
    ] == [
        "q.ndjson.2",
        "q.ndjson.1",
        "q.ndjson",
    ]

    combined = "".join(
        item.read_text()
        for item in paths
    )

    assert combined == (
        "AAAAAAAAAA\n"
        "BBBBBBBBBB\n"
        "CCCCCCCCCC\n"
    )


def test_follow_survives_rotation(
    tmp_path,
):
    async def run():
        path = tmp_path / "q.ndjson"

        append_log_line(
            path,
            "first\n",
            rotate_bytes=8,
            archives=2,
        )

        stream = stream_log_lines(
            path,
            archives=2,
            follow=True,
            poll_interval=0.01,
        )

        try:
            assert (
                await anext(stream)
                == "first"
            )

            append_log_line(
                path,
                "second\n",
                rotate_bytes=8,
                archives=2,
            )

            second = await asyncio.wait_for(
                anext(stream),
                timeout=1,
            )

            assert second == "second"

        finally:
            await stream.aclose()

    asyncio.run(run())


def test_zero_threshold_disables_rotation(
    tmp_path,
):
    path = tmp_path / "q.ndjson"

    append_log_line(
        path,
        "one\n",
        rotate_bytes=0,
        archives=5,
    )

    append_log_line(
        path,
        "two\n",
        rotate_bytes=0,
        archives=5,
    )

    assert (
        path.read_text()
        == "one\ntwo\n"
    )

    assert not (
        tmp_path / "q.ndjson.1"
    ).exists()
