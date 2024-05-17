import pytest
import logging
import quantkit.bt.util.logging


# log test name so that it's easier to debug and fix tests
@pytest.fixture(scope='function', autouse=True)
def test_log(request):
  # Here logging is used, you can use whatever you want to use for logs
  logging.debug("Test '{}' STARTED\n\n".format(request.node.nodeid))
  def fin():
      logging.debug("Test '{}' COMPLETED\n\n".format(request.node.nodeid))
  request.addfinalizer(fin)


