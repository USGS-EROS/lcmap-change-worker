from pyramid import testing as pyr_testing


def test_health():
    from pw.http import health
    request = pyr_testing.DummyRequest()
    response = health(request)
    assert response.status_code == 200
