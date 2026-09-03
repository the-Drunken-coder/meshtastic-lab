from backend.app.gateway.framing import FrameParser, encode_frame


def test_fragmented_header_and_payload() -> None:
    raw = encode_frame(b"payload")
    parser = FrameParser()

    assert parser.feed(raw[:1]) == []
    assert parser.feed(raw[1:3]) == []
    assert parser.feed(raw[3:6]) == []
    frames = parser.feed(raw[6:])

    assert [frame.raw for frame in frames] == [raw]
    assert frames[0].payload == b"payload"


def test_several_frames_in_one_read() -> None:
    parser = FrameParser()
    first = encode_frame(b"one")
    second = encode_frame(b"two")

    assert [frame.raw for frame in parser.feed(first + second)] == [first, second]


def test_corrupt_header_recovers_including_repeated_start_byte() -> None:
    parser = FrameParser()
    raw = encode_frame(b"ok")

    frames = parser.feed(b"boot noise\x94" + raw)

    assert [frame.payload for frame in frames] == [b"ok"]
    assert parser.discarded_bytes > 0


def test_excessive_length_is_rejected_and_parser_resynchronizes() -> None:
    parser = FrameParser()
    oversized = b"\x94\xc3\x02\x01"
    valid = encode_frame(b"ok")

    frames = parser.feed(oversized + b"garbage" + valid)

    assert [frame.payload for frame in frames] == [b"ok"]
    assert parser.oversized_frames == 1


def test_partial_magic_is_retained() -> None:
    parser = FrameParser()

    assert parser.feed(b"noise\x94") == []
    assert [frame.payload for frame in parser.feed(b"\xc3\x00\x01x")] == [b"x"]
