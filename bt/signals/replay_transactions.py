import pandas as pd
import quantkit.bt.core_structure.algo as algo


class ReplayTransactions(algo.Algo):
    """
    Replay a list of transactions that were executed.
    -> take a blotter of actual trades that occurred, measure performance
    -> replay the outputs of backtest.Result.get_transactions()
    -> pass in as additional_data when initializing backtest

    Example
    -------
    transactions =
                                    price	    quantity
        Date	    Security
        2017-01-02	bar	    100.048323	3998.0
                    foo	    102.053214	5879.0
        2017-02-01	bar	    101.132571	-108.0
                    foo	    98.545630	110.0
        2017-03-01	bar	    101.480698	-96.0
                    foo	    94.786230	103.0

    additional_data = {
        "transactions": transactions
    }

    Parameters
    ----------
    transactions: str
        name of a MultiIndex dataframe with format
            - Date, Security | quantity, price
    """

    def __init__(self, transactions: str) -> None:
        super().__init__()
        self.transactions = transactions

    def __call__(self, target) -> bool:
        """
        Run Algo on call ReplayTransactions()

        Parameters
        ----------
        target: Strategy
            strategy of backtest

        Returns
        -------
        bool:
            return True
        """
        timeline = target.data.index
        index = timeline.get_loc(target.now)
        end = target.now
        if index == 0:
            start = pd.Timestamp.min
        else:
            start = timeline[index - 1]
        # Get the transactions since the last update
        all_transactions = target.get_data(self.transactions)
        timestamps = all_transactions.index.get_level_values("Date")
        transactions = all_transactions[(timestamps > start) & (timestamps <= end)]
        for (_, security), transaction in transactions.iterrows():
            c = target[security]
            c.transact(transaction["quantity"], price=transaction["price"])
        return True
