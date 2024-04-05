import quantkit.bt.core_structure.algo as algo


class RollPositionsAfterDates(algo.Algo):
    """
    Roll securities based on provided map.
    -> pass roll map into additional_data when initializing backtest
    -> sets target.perm["rolled"]
    -> keep track of which securities have already been rolled

    Example
    -------
    govt_roll_map =
                    date	    target	        factor
    govt_10Y
    govt_2029_12	2020-03-31	govt_2030_03	1.0
    govt_2030_03	2020-06-30	govt_2030_06	1.0
    govt_2030_06	2020-09-30	govt_2030_09	1.0
    govt_2030_09	2020-12-31	govt_2030_12	1.0
    govt_2030_12	2021-03-31	govt_2031_03	1.0
    govt_2031_03	2021-06-30	govt_2031_06	1.0
    govt_2031_06	2021-09-30	govt_2031_09	1.0

    additional_data = {
        "govt_roll_map" : govt_roll_map,
        }

    RollPositionsAfterDates("govt_roll_map")

    Parameters
    ----------
    roll_data: str
        name of DataFrame indexed by security name, with columns
            - "date": the first date at which the roll can occur
            - "target": the security name we are rolling into
            - "factor": the conversion factor. One unit of the original security
              rolls into "factor" units of the new one.
    """

    def __init__(self, roll_data: str) -> None:
        super().__init__()
        self.run_always = True
        self.roll_data = roll_data

    def __call__(self, target) -> bool:
        """
        Run Algo on call RollPositionsAfterDates()

        Parameters
        ----------
        target: Strategy
            strategy of backtest

        Returns
        -------
        bool:
            return True
        """
        if "rolled" not in target.perm:
            target.perm["rolled"] = set()
        roll_data = target.get_data(self.roll_data)
        transactions = {}
        # Find securities that are candidate for roll
        sec_names = [
            sec_name
            for sec_name, sec in target.children.items()
            if sec._issec
            and sec_name in roll_data.index
            and sec_name not in target.perm["rolled"]
        ]

        # Calculate new transaction and close old position
        for sec_name, sec_fields in roll_data.loc[sec_names].iterrows():
            if sec_fields["date"] <= target.now:
                target.perm["rolled"].add(sec_name)
                new_quantity = sec_fields["factor"] * target[sec_name].position
                new_sec = sec_fields["target"]
                if new_sec in transactions:
                    transactions[new_sec] += new_quantity
                else:
                    transactions[new_sec] = new_quantity
                target.close(sec_name)

        # Do all the new transactions at the end, to do any necessary aggregations first
        for new_sec, quantity in transactions.items():
            # TODO we might want allocate instead of transact here
            target.transact(quantity, new_sec)
        return True
