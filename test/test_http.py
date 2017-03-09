from pyramid import testing as pyr_testing


def test_health():
    from cw.http import health
    request = pyr_testing.DummyRequest()
    response = health(request)
    assert response.status_code == 200