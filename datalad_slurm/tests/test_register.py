from datalad.tests.utils_pytest import assert_result_count


def test_register():
    import datalad.api as da
    assert hasattr(da, 'schedule')
    assert hasattr(da, 'finish')
    assert hasattr(da, 'reschedule')
    assert_result_count(
        da.schedule(cmd="echo test", dry_run="basic"),
        1,
        action='demo')
    assert_result_count(
        da.finish(),
        1,
        action='demo')
    assert_result_count(
        da.reschedule(since="HEAD~1", report=True),
        1,
        action='demo')

