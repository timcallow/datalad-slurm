from datalad.tests.utils_pytest import assert_result_count
import datalad.support.exceptions as dl_exceptions

def test_register():
    import datalad.api as da
    assert hasattr(da, 'schedule')
    assert hasattr(da, 'finish')
    assert hasattr(da, 'reschedule')
    assert_result_count(
        da.schedule(cmd="echo test", dry_run="basic"),
        1,
        status="ok")
    assert_result_count(
        da.finish(),
        0,
        status="ok")
    try:
        da.reschedule(since="HEAD~1", report=True)
        assert False
    except dl_exceptions.IncompleteResultsError:
        assert True
