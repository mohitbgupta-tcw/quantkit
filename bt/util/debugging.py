import quantkit.bt.core_structure.algo as algo
import pdb


class Debug(algo.Algo):
    """
    Activate Debug session using pdb.set_trace.

    Note:
    - use "from IPython import embed; embed()" in command line
    """

    def __call__(self, target) -> bool:
        """
        Run Algo on call Debug()

        Parameters
        ----------
        target: Strategy
            strategy of backtest

        Returns
        -------
        bool:
            return True
        """
        pdb.set_trace()
        return True
