from raftchaos.linearizability import Op, check_key, find_violation


def put(i, value, start, end, key="x"):
    return Op(i, "put", key, value, None, start, end)


def get(i, result, start, end, key="x"):
    return Op(i, "get", key, None, result, start, end)


def test_sequential_history_is_linearizable():
    ops = [put(0, 1, 0, 10), get(1, 1, 20, 30), put(2, 2, 40, 50), get(3, 2, 60, 70)]
    assert check_key(ops) is True


def test_read_of_initial_value_is_linearizable():
    assert check_key([get(0, None, 0, 5), put(1, 1, 10, 20)]) is True


def test_stale_read_after_acknowledged_write_is_rejected():
    ops = [put(0, 1, 0, 10), get(1, None, 20, 30)]
    assert check_key(ops) is False


def test_concurrent_read_may_see_either_value():
    for seen in (None, 1):
        assert check_key([put(0, 1, 0, 100), get(1, seen, 10, 20)]) is True


def test_reads_must_not_go_back_in_time():
    ops = [put(0, 1, 0, 100), get(1, 1, 10, 20), get(2, None, 30, 40)]
    assert check_key(ops) is False


def test_pending_put_may_take_effect():
    ops = [put(0, 7, 0, None), get(1, 7, 50, 60)]
    assert check_key(ops) is True


def test_pending_put_may_be_dropped():
    ops = [put(0, 7, 0, None), get(1, None, 50, 60)]
    assert check_key(ops) is True


def test_value_from_nowhere_is_rejected():
    assert check_key([get(0, 42, 0, 10)]) is False


def test_keys_are_independent():
    history = [put(0, 1, 0, 10, "x"), get(1, None, 20, 30, "y"), get(2, 1, 20, 30, "x")]
    assert find_violation(history) is None
    history.append(get(3, None, 40, 50, "x"))
    assert find_violation(history) == "x"


def test_pending_put_nobody_read_is_ignored():
    ops = [put(0, 1, 0, 10), *[put(i, 100 + i, 5, None) for i in range(1, 30)], get(50, 1, 20, 30)]
    assert check_key(ops) is True
