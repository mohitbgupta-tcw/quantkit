import pytest


# log test name so that it's easier to debug and fix tests
@pytest.fixture(scope='function', autouse=True)
def test_log(request):
  # Here logging is used, you can use whatever you want to use for logs
  print("\n\nTest '{}' STARTED\n\n".format(request.node.nodeid))
  def fin():
      print("\n\nTest '{}' COMPLETED\n\n".format(request.node.nodeid))
  request.addfinalizer(fin)


