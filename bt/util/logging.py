import sys
from quantkit.utils.logging import logging
import quantkit.bt.core_structure.algo as algo


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
        logging.warning("WORKING ON: %s", target.now)
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
        logging.warning("Temp: %s", target.temp)
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
            logging.warning("Risk: %s, %s", target.now, target.risk)
        return True
