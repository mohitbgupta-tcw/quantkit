import logging
import sys
import quantkit.bt.core_structure.algo as algo

logger = logging.getLogger()
logger.level = logging.DEBUG
stream_handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)


class PrintDate(algo.Algo):
    """
    Log current date of strategy
    """

    def __call__(self, target) -> bool:
        """
        Run Algo on call PrintDate()

        Parameters
        ----------
        target: Strategy
            strategy of backtest

        Returns
        -------
        bool:
            return True
        """
        logger.warning("WORKING ON: %s", target.now)
        return True


class PrintTempData(algo.Algo):
    """
    Log temp data of strategy
    """

    def __call__(self, target) -> bool:
        """
        Run Algo on call PrintTempData()

        Parameters
        ----------
        target: Strategy
            strategy of backtest

        Returns
        -------
        bool:
            return True
        """
        logger.warning("Temp: %s", target.temp)
        return True


class PrintRisk(algo.Algo):
    """
    Log risk data of strategy
    """

    def __call__(self, target) -> bool:
        """
        Run Algo on call PrintRisk()

        Parameters
        ----------
        target: Strategy
            strategy of backtest

        Returns
        -------
        bool:
            return True
        """
        if hasattr(target, "risk"):
            logger.warning("Risk: %s, %s", target.now, target.risk)
        return True
