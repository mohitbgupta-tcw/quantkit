import numpy as np
import pandas as pd
from typing import Union
import datetime
import quantkit.bt.util.util_functions as util_functions
from quantkit.bt.core_structure import algo, node
import quantkit.bt.portfolio_management.comission_functions as comission_functions


class StrategyBase(node.Node):
    """
    Strategy Node:
        - define strategy logic within tree
        - role: allocate capital to children

    Parameters
    ----------
    name: str
        strategy name
    children: dict | list
        collection of children
    parent: Node, optional
        parent Node
    PAR: int, optional
        par value of strategy
    """

    def __init__(
        self,
        name: str,
        children: Union[list, dict],
        parent: node.Node = None,
        PAR: int = 100,
    ):
        self._strat_children = []
        super().__init__(name, children=children, parent=parent)
        self._weight = 1
        self._value = 0
        self._notl_value = 0
        self._price = PAR

        # helper vars
        self._net_flows = 0
        self._last_value = 0
        self._last_notl_value = 0
        self._last_price = PAR
        self._last_fee = 0
        self._last_chk = 0
        self._positions = None
        self.bankrupt = False
        self._accrued_interest = 0
        self._last_accrued_interest = 0

        # default commission function
        self.commission_fn = comission_functions.dflt_comm_fn

    @property
    def price(self) -> float:
        """
        Returns
        -------
        float:
            current price of Strategy
        """
        return self._price

    @property
    def prices(self) -> pd.DataFrame:
        """
        Returns
        -------
        pd.DataFrame:
            TimeSeries of Node's price
        """
        return self._prices.loc[: self.now]

    @property
    def values(self) -> pd.DataFrame:
        """
        Returns
        -------
        pd.DataFrame:
            TimeSeries of (dollar) values of Strategy
        """
        return self._values.loc[: self.now]

    @property
    def notional_values(self) -> pd.DataFrame:
        """
        Returns
        -------
        pd.DataFrame:
            TimeSeries of security's notional value
        """
        return self._notl_values.loc[: self.now]

    @property
    def capital(self) -> float:
        """
        Returns
        -------
        float:
            current capital - amount of unallocated capital left in strategy
        """
        return self._capital

    @property
    def cash(self) -> pd.DataFrame:
        """
        Returns
        -------
        pd.DataFrame:
            TimeSeries of unallocated capital
        """
        return self._cash

    @property
    def fees(self) -> pd.DataFrame:
        """
        pd.DataFrame:
            TimeSeries of fees
        """
        return self._fees.loc[: self.now]

    @property
    def flows(self) -> pd.DataFrame:
        """
        Returns
        -------
        pd.DataFrame:
            TimeSeries of flows
        """
        return self._all_flows.loc[: self.now]

    @property
    def bidoffer_paid(self) -> float:
        """
        Returns
        -------
        float:
            bid/offer spread paid on transactions in current step
        """
        if self._bidoffer_set:
            return self._bidoffer_paid
        else:
            raise Exception(
                "Bid/offer accounting not turned on: "
                '"bidoffer" argument not provided during setup'
            )

    @property
    def bidoffers_paid(self) -> pd.DataFrame:
        """
        Returns
        -------
        pd.DataFrame:
            TimeSeries of bid/offer spread paid on transactions in current step
        """
        if self._bidoffer_set:
            return self._bidoffers_paid.loc[: self.now]
        else:
            raise Exception(
                "Bid/offer accounting not turned on: "
                '"bidoffer" argument not provided during setup'
            )

    @property
    def universe(self) -> pd.DataFrame:
        """
        Returns
        -------
        pd.DataFrame:
            data universe available at current time
        """
        if self.now == self._last_chk:
            return self._funiverse
        else:
            self._last_chk = self.now
            self._funiverse = self._universe.loc[: self.now]
            return self._funiverse

    @property
    def securities(self) -> list:
        """
        Returns
        -------
        list:
            list of children that are Security
        """
        return [x for x in self.members if x._issec]

    @property
    def outlays(self) -> pd.DataFrame:
        """
        Returns
        -------
        pd.DataFrame:
            outlays for each child Security
        """
        outlays = pd.DataFrame()
        for x in self.securities:
            if x.name in outlays.columns:
                outlays[x.name] += x.outlays
            else:
                outlays[x.name] = x.outlays
        return outlays

    @property
    def positions(self) -> pd.DataFrame:
        """
        Returns
        -------
        pd.DataFrame:
            TimeSeries of positions
        """
        vals = pd.DataFrame()
        for x in self.members:
            if x._issec:
                if x.name in vals.columns:
                    vals[x.name] += x.positions
                else:
                    vals[x.name] = x.positions
        self._positions = vals
        return vals

    def setup(self, prices: pd.DataFrame, **kwargs) -> None:
        """
        Setup Strategy

        Parameters
        ----------
        prices: pd.DataFrame
            DataFrame of prices for securities in universe
        """
        # save full universe in case we need it
        self._original_data = prices
        self._setup_kwargs = kwargs

        # setup universe
        funiverse = prices.copy()
        valid_filter = list(set(prices.columns).intersection(self.children.keys()))
        funiverse = prices[valid_filter].copy()

        # if we have strat children, we will need to create their columns
        for c in self._strat_children:
            funiverse[c] = np.nan
        funiverse = pd.DataFrame(funiverse)

        self._universe = funiverse
        # holds filtered universe
        self._funiverse = funiverse
        self._last_chk = None

        # We're not bankrupt yet
        self.bankrupt = False

        # setup internal data
        self.data = pd.DataFrame(
            index=funiverse.index,
            columns=["price", "value", "notional_value", "cash", "fees", "flows"],
            data=0.0,
        )

        self._prices = self.data["price"]
        self._values = self.data["value"]
        self._notl_values = self.data["notional_value"]
        self._cash = self.data["cash"]
        self._fees = self.data["fees"]
        self._all_flows = self.data["flows"]

        if "bidoffer" in kwargs:
            self._bidoffer_set = True
            self.data["bidoffer_paid"] = 0.0
            self._bidoffers_paid = self.data["bidoffer_paid"]

        # setup children as well
        if self.children is not None:
            for c in self._childrenv:
                c.setup(prices, **kwargs)

    def get_data(self, key: str) -> pd.DataFrame:
        """
        Get data that was passed to the setup function via kwargs for use in the Algos.

        Parameters
        ----------
        key: str
            name of data

        Returns
        -------
        pd.DataFrame
            DataFrame of type key
        """
        return self._setup_kwargs[key]

    def value_update(self, date: datetime.date, inow: int) -> None:
        """
        Reset:
            - values
            - weights

        Parameters
        ----------
        date: datetime.date
            current date
        inow: int
            current location in price DataFrame
        """
        self._value = self._capital  # leftover capital from last rebalance
        self._last_accrued_interest = self._accrued_interest
        self._accrued_interest = 0

        # update data by collecting values from children
        for c in self._childrenv:
            # update child Node
            c.pre_settlement_update(date, inow)
            # sweep up cash from Security Node (coupon payments, etc)
            if c._issec:
                self._capital += c._capital
                self._value += c._capital
                c._capital = 0
                self._accrued_interest += c._accrued_interest

            # update portfolio values
            self._value += c.value

        # update DataFrame
        self._values.values[inow] = self._value + self._accrued_interest

        # update children weights
        for c in self._childrenv:
            if not util_functions.is_zero(self._value):
                c._weight = c.value / self._value
            else:
                c._weight = 0.0

    def pre_settlement_update(self, date: datetime.date, inow: int) -> None:
        """
        Update strategy before running Rebalance()
        Reset:
            - date
            - prices
            - values
            - fees
            - coupons

        Parameters
        ----------
        date: datetime.date
            current date
        inow: int
            current location in price DataFrame
        """
        # Reset Values
        self.now = date
        self.inow = inow
        self._last_price = self._price
        self._last_value = self._value
        self._last_notl_value = self._notl_value
        self._last_fee = 0.0

        self.value_update(self.now, self.inow)

        # Declare a bankruptcy
        if self.root == self:
            if (
                (self._value < 0)
                and not util_functions.is_zero(self._value)
                and not self.bankrupt
            ):
                self.bankrupt = True
                self.flatten()

        # calculate portfolio return and update price
        bottom = self._last_value + self._net_flows + self._last_accrued_interest
        if not util_functions.is_zero(bottom):
            ret = (self._value + self._accrued_interest) / (bottom) - 1
        else:
            if util_functions.is_zero(self._value):
                ret = 0
            else:
                raise ZeroDivisionError(
                    "Could not update %s on %s. Last value "
                    "was %s and net flows were %s. Current"
                    "value is %s. Therefore, "
                    "we are dividing by zero to obtain the return "
                    "for the period."
                    % (
                        self.name,
                        self.now,
                        self._last_value,
                        self._net_flows,
                        self._value,
                    )
                )
        self._price = self._last_price * (1 + ret)
        self._prices.values[inow] = self._price

        # if we have strategy children, we will need to update strategy price in universe
        for c in self._strat_children:
            self._universe.loc[date, c] = self.children[c].price

    def post_settlement_update(self) -> None:
        """
        Reset:
            - bidoffers
            - notional values
        Set:
            - cash
            - fees
            - flows
        """
        self._notl_value = 0.0
        for c in self._childrenv:
            c.post_settlement_update()
            self._notl_value += abs(c.notional_value)
            self._notl_values.values[self.inow] = self._notl_value

            if self._bidoffer_set:
                self._bidoffer_paid += c.bidoffer_paid

        if self._bidoffer_set:
            self._bidoffers_paid.values[self.inow] = self._bidoffer_paid
            self._bidoffer_paid = 0

        # update leftover cash, fees and inflows
        self._cash.values[self.inow] = self._capital
        self._fees.values[self.inow] = self._last_fee
        self._all_flows.values[self.inow] = self._net_flows
        self._net_flows = 0

    def adjust(self, amount: float, flow: bool = True, fee: float = 0.0) -> None:
        """
        Inject capital into a Strategy.
        If injection is flow (p.e. monthly contribution), it won't have impact on performance of strategy.
        If injection is non-flow (p.e. comission, dividend), it will impact performance of strategy.

        Parameters
        ----------
        amount: float
            dollar amount to adjust capital by
        flow: bool
            injection is flow
        fee: float, optional
            fee paid for adjustment
        """
        self._capital += amount
        self._last_fee += fee

        if flow:
            self._net_flows += amount

    def allocate(self, amount: float, child: str = None) -> None:
        """
        Allocate capital to Strategy.
        - capital is allocated recursively down the children
        - if child is specified, capital will be allocated to that specific child
        - deduct same amount from parent's capital

        This is used in strategy of strategies only.

        Parameters
        ----------
        amount: float
            dollar amount to allocate
        child: str, optional
            name of child -  if specified, allocation will be directed to child only
        """
        if child is not None:
            self.children[child].allocate(amount)
        else:
            if self.parent == self:
                self.parent.adjust(-amount, flow=True)
            else:
                self.parent.adjust(-amount, flow=False)

            self.adjust(amount, flow=True)
            self.value_update(self.now, self.inow)

            if self.children is not None:
                for c in self._childrenv:
                    c.allocate(amount * c._weight)

    def transact(self, q: float, child: str = None) -> None:
        """
        Transact notional amount q in Strategy.
        - capital is allocated recursively down the children
        - if child is specified, capital will be allocated to that specific child

        Parameters
        ----------
        q: float
            notional quantity
        child: str
            name of child -  if specified, allocation will be directed to child only
        """
        if child is not None:
            self.children[child].transact(q)
        else:
            if self.children is not None:
                [c.transact(q * c._weight) for c in self._childrenv]

    def rebalance(self, child, weight, base) -> None:
        """
        Rebalance a child to a given weight

        Parameters
        ----------
        weight: float
            target weight of child (usually in range -1.0 to 1.0)
        child: str
            name of child
        base: float
            base amount for weight delta calculations
        """
        # close child
        if util_functions.is_zero(weight):
            return self.close(child)

        c = self.children[child]
        delta = weight - c.weight
        c.allocate(delta * base)

    def close(self, child: str) -> None:
        """
        Close a child's position
        - alias for rebalance(0, child)
        - will also flatten all child's children

        Parameters
        ----------
        child str:
            name of child
        """
        c = self.children[child]
        if c.children:
            c.flatten()

        if c.value != 0.0 and not np.isnan(c.value):
            c.allocate(-c.value)

    def flatten(self) -> None:
        """
        Close all children positions
        """
        [c.allocate(-c.value) for c in self._childrenv]

    def run(self) -> None:
        """
        None
        """
        pass

    def set_commissions(self, fn) -> None:
        """
        Set commission (transaction fee) function.
        Can be lambda function or set in bt.portfolio_management.comission_functions (preferred way).

        Parameters
        ----------
        fn: function
            function taking in price and quantity used to determine commission amount

        """
        self.commission_fn = fn

        for c in self._childrenv:
            if isinstance(c, StrategyBase):
                c.set_commissions(fn)

    def get_transactions(self) -> pd.DataFrame:
        """
        Returns the transactions of strategy as a MultiIndex DataFrame

        Format:
        Date, Security | quantity, price

        Returns
        -------
        pd.DataFrame:
            all transactions
        """
        prc = pd.DataFrame({x.name: x.prices for x in self.securities}).unstack()

        positions = pd.DataFrame()
        for x in self.securities:
            if x.name in positions.columns:
                positions[x.name] += x.positions
            else:
                positions[x.name] = x.positions
        trades = positions.diff()
        trades.iloc[0] = positions.iloc[0]
        trades = trades[trades != 0].unstack().dropna()

        # Adjust prices for bid/offer paid if needed
        if self._bidoffer_set:
            bidoffer = pd.DataFrame(
                {x.name: x.bidoffers_paid for x in self.securities}
            ).unstack()
            prc += bidoffer / trades

        res = pd.DataFrame({"price": prc, "quantity": trades}).dropna(
            subset=["quantity"]
        )
        res.index.names = ["Security", "Date"]
        res = res.swaplevel().sort_index()

        return res


class Strategy(StrategyBase):
    """
    Strategy expands on the StrategyBase and incorporates stack of Algos.

    Parameters
    ----------
    name: str
        strategy name
    children: dict | list
        collection of children
    algos: list
        list of Algos to be passed into AlgoStack
    parent: Node, optional
        parent Node
    PAR: int, optional
        par value of strategy
    """

    def __init__(
        self,
        name: str,
        children: Union[list, dict],
        algos: list = None,
        parent: node.Node = None,
        PAR: float = 100,
    ) -> None:
        super().__init__(name, children=children, parent=parent, PAR=PAR)
        if algos is None:
            algos = []
        self.stack = algo.AlgoStack(*algos)
        self.temp = {}
        self.perm = {}

    def run(self) -> None:
        """
        Run AlgoStack
        """
        if "weights" in self.temp:
            self.temp = {"weights": self.temp["weights"]}
        else:
            self.temp = {}

        # run algo stack
        self.stack(self)

        # run children
        for c in self._childrenv:
            c.run()


class FixedIncomeStrategy(Strategy):
    """
    FixedIncomeStrategy

    Parameters
    ----------
    name: str
        strategy name
    children: dict | list
        collection of children
    algos: list
        list of Algos to be passed into AlgoStack
    parent: Node, optional
        parent Node
    PAR: int, optional
        par value of strategy
    """

    def __init__(
        self,
        name: str,
        children: Union[list, dict],
        algos: list = None,
        parent: node.Node = None,
        PAR: float = 100,
    ) -> None:
        super().__init__(name, algos=algos, children=children, parent=parent, PAR=PAR)
        self._fixed_income = True
