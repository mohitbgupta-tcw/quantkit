from typing import Union
from __future__ import annotations
import pandas as pd
import numpy as np
import math
import datetime
import quantkit.utils.util_functions as util_functions


class Node(object):
    """
    Main building block in tree structure design
    --> main functionality of tree node

    Parameters
    ----------
    name: str
        node name
    parent: Node
        parent Node
    children: dict | list
        collection of children
    """

    def __init__(
        self, name: str, parent: Node = None, children: Union[list, dict] = None
    ) -> None:
        self.name = name
        self.integer_positions = True

        # children
        self.children = {}
        self._universe_tickers = []
        self._childrenv = []  # Shortcut to self.children.values()
        self._add_children(children=children)

        # parent
        self._add_parent(parent=parent)

        self.now = 0
        self.root.stale = False  # used to avoid unnecessary update

        # helper vars
        self._price = 0
        self._value = 0
        self._notl_value = 0
        self._weight = 0
        self._capital = 0

        # is security flag - used to avoid updating 0 pos securities
        self._issec = False
        self._fixed_income = False
        # flag for whether to do bid/offer accounting
        self._bidoffer_set = False
        self._bidoffer_paid = 0

    def _add_parent(self, parent: Node) -> None:
        """
        Add parent Node to Node and set root.
        If no parent is provided, make Node parent of itself.

        Parameters
        ----------
        parent: Node
            parent Node
        """
        if parent is None:
            self.parent = self
            self.root = self
        else:
            self.parent = parent
            parent._add_children([self])

    def _set_root(self, root: Node) -> None:
        """
        Set root for Node

        Parameters
        ----------
        root: Node
            root Node
        """
        self.root = root
        for c in self._childrenv:
            c._set_root(root)

    def _add_children(self, children: Union[list, dict]) -> None:
        """
        Add children to Node

        Parameters
        ----------
        children: list | dict
            collection of children
        """
        if children is not None:
            if isinstance(children, dict):
                children = list(children.values())

            for child in children:
                if child.name in self.children:
                    raise ValueError(f"Child {child} already exists")

                child.parent = self
                child._set_root(self.root)
                child.use_integer_positions(self.integer_positions)

                self.children[child.name] = child
                self._childrenv.append(child)

                # if strategy, turn on flag and add name to list
                if isinstance(child, StrategyBase):
                    self._has_strat_children = True
                    self._strat_children.append(child.name)

    def use_integer_positions(self, integer_positions: bool) -> None:
        """
        Use (or not) integer positions only

        Parameters
        ----------
        integer_position: bool
            use integer positions
        """
        self.integer_positions = integer_positions
        for c in self._childrenv:
            c.use_integer_positions(integer_positions)

    @property
    def fixed_income(self) -> bool:
        """
        Returns
        -------
        bool:
            Node is Fixed Income
        """
        return self._fixed_income

    @property
    def prices(self) -> pd.DataFrame:
        """
        Returns
        -------
        pd.DataFrame:
            TimeSeries of Node's price
        """
        raise NotImplementedError()

    @property
    def price(self) -> float:
        """
        Returns
        -------
        float:
            current price of Node
        """
        raise NotImplementedError()

    @property
    def value(self) -> float:
        """
        Returns
        -------
        float:
            current (dollar) value of Node
        """
        if self.root.stale:
            self.root.update(self.root.now, None)
        return self._value

    @property
    def notional_value(self) -> float:
        """
        Returns
        -------
        float:
            current notional (dollar) value of Node
        """
        if self.root.stale:
            self.root.update(self.root.now, None)
        return self._notl_value

    @property
    def weight(self) -> float:
        """
        Returns
        -------
        float:
            current weight of Node with respect to parent
        """
        if self.root.stale:
            self.root.update(self.root.now, None)
        return self._weight

    def setup(self, prices: pd.DataFrame, **kwargs) -> None:
        """
        Setup method used to initialize a Node

        Parameters
        ----------
        prices: pd.DataFrame
            DataFrame of prices for securities in universe
        """
        raise NotImplementedError()

    def update(self, date: datetime.date, inow: int = None) -> None:
        """
        Update Node with latest date, and optionally data

        Parameters
        ----------
        date: datetime.date
            current date
        inow: int, optional
            current integer position
        """
        raise NotImplementedError()

    def adjust(self, amount: float, update: bool = True, flow: bool = True) -> None:
        """
        Adjust Node value by amount

        Parameters
        ----------
        amount: float
            adjustment amount
        update: bool
            force update
        flow: bool
            adjustment is flow (capital injections)
            -> no impact on performance
            -> should not be reflected in returns
            adjustment is non-flow (comission, dividend)
            -> impact performance
        """
        raise NotImplementedError()

    def allocate(self, amount: float, update=True) -> None:
        """
        Allocate capital to Node

        Parameters
        ----------
        amount: float
            allocation amount
        update: bool
            force update
        """
        raise NotImplementedError()

    @property
    def members(self) -> list:
        """
        Node members. Members include current node as well as Node's
        children.

        Returns
        -------
        list:
            list of all nodes hanging under Node in tree
        """
        res = [self]
        for c in self._childrenv:
            res.extend(c.members)
        return res

    @property
    def full_name(self):
        if self.parent == self:
            return self.name
        else:
            return f"{self.parent.full_name}>{self.name}"

    def __repr__(self):
        return f"<{self.__class__.__name__} {self.full_name}>"


class StrategyBase(Node):
    """ """

    def __init__(
        self, name: str, parent: Node = None, children: Union[list, dict] = None
    ) -> None:
        super().__init__(name, parent, children)

        self._strat_children = []


class SecurityBase(Node):
    """
    Security Node:
        - models asset that can be bought or sold
        - define security within tree
        - security has no children

    Parameters
    ----------
    name: str
        security name
    multiplier: float
        security multiplier - typically used for derivatives or to trade in lots
    """

    def __init__(self, name: str, multiplier: float = 1) -> None:
        super().__init__(name, parent=None, children=None)
        self.multiplier = multiplier

        self._position = 0
        self._last_pos = 0
        self._issec = True
        self._outlay = 0
        self._bidoffer = 0

    @property
    def price(self) -> float:
        """
        Returns
        -------
        float:
            current price of security
        """
        self.update()
        return self._price

    @property
    def prices(self) -> pd.DataFrame:
        """
        Returns
        -------
        pd.DataFrame:
            TimeSeries of security's price
        """
        self.update()
        return self._prices.loc[: self.now]

    @property
    def values(self) -> pd.DataFrame:
        """
        Returns
        -------
        pd.DataFrame:
            TimeSeries of security's value
            value = position * price * multiplier
        """
        self.update()
        if self.root.stale:
            self.root.update(self.root.now, None)
        return self._values.loc[: self.now]

    @property
    def notional_values(self) -> pd.DataFrame:
        """
        Returns
        -------
        pd.DataFrame:
            TimeSeries of security's notional value
        """
        self.update()
        if self.root.stale:
            self.root.update(self.root.now, None)
        return self._notl_values.loc[: self.now]

    @property
    def position(self) -> float:
        """
        Returns
        -------
        float:
            current position (quantity)
        """
        return self._position

    @property
    def positions(self) -> pd.DataFrame:
        """
        Returns
        -------
        pd.DataFrame:
            TimeSeries of security's positions
        """
        if self.root.stale:
            self.root.update(self.root.now, None)
        return self._positions.loc[: self.now]

    @property
    def outlays(self) -> pd.DataFrame:
        """
        Returns
        -------
        pd.DataFrame:
            TimeSeries of outlays:
                - Positive outlays (buys): security received and consumed capital
                - Negative outlays: security closed/sold, returned capital to parent
        """
        self.update()
        if self.root.stale:
            self.root.update(self.root.now, None)
        return self._outlays.loc[: self.now]

    @property
    def bidoffer(self) -> float:
        """
        Returns
        -------
        float:
            current bid/offer spread
        """
        self.update()
        return self._bidoffer

    @property
    def bidoffer_paid(self) -> float:
        """
        Returns
        -------
        float:
            bid/offer spread paid on transactions in current step
        """
        self.update()
        return self._bidoffer_paid

    @property
    def bidoffers_paid(self) -> pd.DataFrame:
        """
        Returns
        -------
        pd.DataFrame:
            TimeSeries of bid/offer spread paid on transactions in current step
        """
        if self._bidoffer_set:
            self.update(self.root.now)
            if self.root.stale:
                self.root.update(self.root.now, None)
            return self._bidoffers_paid.loc[: self.now]
        else:
            raise Exception(
                "Bid/offer accounting not turned on: "
                '"bidoffer" argument not provided during setup'
            )

    def setup(
        self, prices: pd.DataFrame, bidoffer: pd.DataFrame = None, **kwargs
    ) -> None:
        """
        Setup Security

        Parameters
        ----------
        prices: pd.DataFrame
            DataFrame of prices for securities in universe
        bidoffer: pd.DataFrame, optional
            bid/offer spread for securities in universe across time
        """
        self._prices = prices[self.name]
        self.data = pd.DataFrame(
            index=self._prices.index,
            columns=["value", "position", "notional_value", "outlay"],
            data=0.0,
        )

        self._values = self.data["value"]
        self._notl_values = self.data["notional_value"]
        self._positions = self.data["position"]
        self._outlays = self.data["outlay"]

        # save bidoffer, if provided
        if bidoffer is not None:
            self._bidoffer_set = True
            if self.name in bidoffer.columns:
                bidoffers = bidoffer[self.name]
                if bidoffers.index.equals(prices.index):
                    self._bidoffers = bidoffers
                    self.data["bidoffer"] = self._bidoffers
                else:
                    raise ValueError("Index of bidoffer must match price data")
            else:
                bidoffers = None
                self.data["bidoffer"] = 0.0
                self._bidoffers = self.data["bidoffer"]

            self.data["bidoffer_paid"] = 0.0
            self._bidoffers_paid = self.data["bidoffer_paid"]

    def update(self, date=None, inow=None) -> None:
        """
        Update Node with latest date, and optionally data
        Update:
            - price
            - value
            - weight
            - etc.

        Parameters
        ----------
        date: datetime.date, optional
            current date
        inow: int, optional
            current integer position
        """
        if date is None:
            date = self.parent.now
        if date == self.now and self._last_pos == self._position:
            return

        if inow is None:
            if date == 0:
                inow = 0
            else:
                inow = self.data.index.get_loc(date)

        # date change - update price
        if date != self.now:
            self.now = date
            self._price = self._prices.values[inow]

            # update bid/offer
            if self._bidoffer_set:
                self._bidoffer = self._bidoffers.values[inow]
                self._bidoffer_paid = 0.0

        # update position
        self._positions.values[inow] = self._position
        self._last_pos = self._position

        # update value
        self._value = self._position * self._price * self.multiplier
        self._notl_value = self._value
        self._values.values[inow] = self._value
        self._notl_values.values[inow] = self._notl_value

        # update outlay
        self._outlays.values[inow] += self._outlay
        self._outlay = 0

        if self._bidoffer_set:
            self._bidoffers_paid.values[inow] = self._bidoffer_paid

    def allocate(self, amount: float, update: bool = True) -> None:
        """
        Allocate capital to security -> calculate amount of shares to sell/ buy
            - given amount of shares will be determined on current price
            - commission will be calculated based on the parent's commission function
            - any remaining capital will be passed back up to parent as adjustment

        Logic:
        - positive amount: maximum amount security can use up for a purchase
            --> commissions push above this amount
            --> cannot buy `q`, must decrease its value

        - negative amount: want to 'raise' at least the amount indicated
            --> if commission, must sell additional units to fund requirement
            --> q must decrease

        Parameters
        ----------
        amount: float
            amount of adjustment
        update: bool, optional
            force update
        """
        self.update()
        if util_functions.is_zero(amount):
            return

        if self.parent is self or self.parent is None:
            raise Exception("Cannot allocate capital to a parentless security")

        if util_functions.is_zero(self._price) or np.isnan(self._price):
            raise Exception(
                f"Cannot allocate capital to {self.name} because price is {self._price} as of {self.parent.now}"
            )

        # closing out?
        if util_functions.is_zero(amount + self._value):
            q = -self._position
        else:
            q = amount / (self._price * self.multiplier)
            if self.integer_positions:
                # long position
                if (self._position > 0) or (
                    util_functions.is_zero(self._position) and (amount > 0)
                ):
                    q = math.floor(q)
                # short position
                else:
                    q = math.ceil(q)

        if util_functions.is_zero(q) or np.isnan(q):
            return

        if not q == -self._position:
            full_outlay, _, _, _ = self.outlay(q)

            # if full outlay > amount, we must decrease the magnitude of `q`
            i = 0
            last_q = q
            last_amount_short = full_outlay - amount
            while not np.isclose(full_outlay, amount, rtol=1e-16) and q != 0:
                dq_wout_considering_tx_costs = (full_outlay - amount) / (
                    self._price * self.multiplier
                )
                q = q - dq_wout_considering_tx_costs

                if self.integer_positions:
                    q = math.floor(q)

                full_outlay, _, _, _ = self.outlay(q)

                if self.integer_positions:
                    full_outlay_of_1_more, _, _, _ = self.outlay(q + 1)

                    if full_outlay < amount and full_outlay_of_1_more > amount:
                        break

                i = i + 1
                if i > 1e4:
                    raise Exception(
                        "Potentially infinite loop detected. This occurred "
                        "while trying to reduce the amount of shares purchased"
                        " to respect the outlay <= amount rule. This is most "
                        "likely due to a commission function that outputs a "
                        "commission that is greater than the amount of cash "
                        "a short sale can raise."
                    )

                if self.integer_positions and last_q == q:
                    raise Exception(
                        "Newton Method like root search for quantity is stuck!"
                        " q did not change in iterations so it is probably a bug"
                        " but we are not entirely sure it is wrong! Consider "
                        " changing to warning."
                    )
                last_q = q

                if np.abs(full_outlay - amount) > np.abs(last_amount_short):
                    raise Exception(
                        "The difference between what we have raised with q and"
                        " the amount we are trying to raise has gotten bigger since"
                        " last iteration! full_outlay should always be approaching"
                        " amount! There may be a case where the commission fn is"
                        " not smooth"
                    )
                last_amount_short = full_outlay - amount

        self.transact(q, update, False)

    def transact(self, q: float, update=True, update_self=True, price=None) -> None:
        """
        Transacts security --> buy/sell the security for a given quantity
            - commission will be calculated based on the parent's commission function
            - any remaining capital will be passed back up to parent as adjustment

        Parameters
        ----------
        q: float
            Quantity of shares to buy/ sell
        update: bool, optional
            force update on parent
        update_self: bool
            force update on self
        """
        if update_self:
            self.update()

        # if q is 0 nothing to do
        if util_functions.is_zero(q) or np.isnan(q):
            return

        if price is not None and not self._bidoffer_set:
            raise ValueError(
                'Cannot transact at custom prices when "bidoffer" has '
                "not been passed during setup to enable bid-offer tracking."
            )

        # adjust position & value
        self._position += q

        # calculate adjustment for parent
        full_outlay, outlay, fee, bidoffer = self.outlay(q, p=price)

        # store outlay for future reference
        self._outlay += outlay
        self._bidoffer_paid += bidoffer

        # call parent
        self.parent.adjust(-full_outlay, update=update, flow=False, fee=fee)

    def commission(self, q: float, p: float) -> None:
        """
        Commission (transaction fee) based on quantity and price.

        Parameters
        ----------
        q: float
            quantity
        p: float
            price
        """
        return self.parent.commission_fn(q, p)

    def outlay(self, q: float, p: float = None):
        """
        Complete cash outlay (including commission) necessary given a quantity q.

        Parameters
        ----------
        q: float
            quantity
        p: float, optional
            price override

        Returns
        -------
        float:
            complete cash outlay including comission
        float:
            cash outlay excluding fees
        float:
            comission fees
        float:
            bidoffer
        """
        if p is None:
            fee = self.commission(q, self._price * self.multiplier)
            bidoffer = abs(q) * 0.5 * self._bidoffer * self.multiplier
        else:
            # price override provided: custom transaction
            fee = self.commission(q, p * self.multiplier)
            bidoffer = q * (p - self._price) * self.multiplier

        outlay = q * self._price * self.multiplier + bidoffer

        return outlay + fee, outlay, fee, bidoffer

    def run(self) -> None:
        """
        Does nothing - securities have nothing to do on run.
        """
        pass


class Security(SecurityBase):
    """
    Standard security

    Parameters
    ----------
    name: str
        security name
    multiplier: float
        security multiplier - typically used for derivatives or to trade in lots
    """

    def __init__(self, name: str, multiplier: float = 1) -> None:
        super().__init__(name, multiplier)


class FixedIncomeSecurity(SecurityBase):
    """
    Fixed Income Security

    Parameters
    ----------
    name: str
        security name
    multiplier: float
        security multiplier - typically used for derivatives or to trade in lots
    """

    def __init__(self, name: str, multiplier: float = 1) -> None:
        super().__init__(name, multiplier)

    def update(self, date=None, inow=None):
        """
        Update Node with latest date, and optionally data

        Notional value is measured only based on the quantity (par value) of the security

        Update:
            - price
            - value
            - weight
            - etc.

        Parameters
        ----------
        date: datetime.date, optional
            current date
        inow: int, optional
            current integer position
        """
        if inow is None:
            if date == 0:
                inow = 0
            else:
                inow = self.data.index.get_loc(date)

        super().update(date, inow)

        # For fixed income securities (bonds, swaps), notional value is position size, not value!
        self._notl_value = self._position
        self._notl_values.values[inow] = self._notl_value


class CouponPayingSecurity(FixedIncomeSecurity):
    """
    Securities which pay (possibly irregular) coupons (or other forms of cash disbursement)
    -> coupon payments adjust the capital of the parent
    -> coupons and costs must be passed in during setup

    Parameters
    ----------
    name: str
        security name
    multiplier: float
        security multiplier - typically used for derivatives or to trade in lots
    fixed_income: bool, optional
        notional_value is based only on quantity, or on market value
    """

    def __init__(self, name, multiplier=1, fixed_income=True):
        super(CouponPayingSecurity, self).__init__(name, multiplier)
        self._coupon = 0
        self._holding_cost = 0
        self._fixed_income = fixed_income

    def setup(
        self,
        prices: pd.DataFrame,
        coupons: pd.DataFrame,
        cost_long: pd.DataFrame = None,
        cost_short: pd.DataFrame = None,
        **kwargs,
    ) -> None:
        """
        Setup Security with price and coupon data

        Parameters
        ----------
        prices: pd.DataFrame
            DataFrame of prices for securities in universe
        coupons: pd.DataFrame
            DataFrame of coupon/carry amount
        cost_long: pd.DataFrame, optional
            cost of holding a unit long position in the security
        cost_short: pd.DataFrame, optional
            cost of holding a unit short position in the security
        """
        super().setup(prices, **kwargs)

        if self.name in coupons.columns:
            self._coupons = kwargs["coupons"][self.name]
        else:
            self._coupons = None

        if self._coupons is None or not self._coupons.index.equals(prices.index):
            raise ValueError("Index of coupons must match universe data")

        # Handle holding costs
        if cost_long is not None:
            self._cost_long = cost_long[self.name]
        else:
            self._cost_long = None
        if cost_short is not None:
            self._cost_short = cost_short[self.name]
        else:
            self._cost_short = None

        self.data["coupon"] = 0.0
        self.data["holding_cost"] = 0.0
        self._coupon_income = self.data["coupon"]
        self._holding_costs = self.data["holding_cost"]

    def update(self, date, inow=None) -> None:
        """
        Update Node with latest date, and optionally data
        Update:
            - price
            - value
            - weight
            - etc.

        Parameters
        ----------
        date: datetime.date, optional
            current date
        inow: int, optional
            current integer position
        """
        if inow is None:
            if date == 0:
                inow = 0
            else:
                inow = self.data.index.get_loc(date)

        # Standard update
        super().update(date, inow)

        coupon = self._coupons.values[inow]
        # If we were to call self.parent.adjust, then all the child weights would
        # need to be updated. If each security pays a coupon, then this happens for
        # each child. Instead, we store the coupon on self._capital, and it gets
        # swept up as part of the strategy update

        if np.isnan(coupon):
            self._coupon = 0.0
        else:
            self._coupon = self._position * coupon

        if self._position > 0 and self._cost_long is not None:
            cost = self._cost_long.values[inow]
            self._holding_cost = self._position * cost
        elif self._position < 0 and self._cost_short is not None:
            cost = self._cost_short.values[inow]
            self._holding_cost = -self._position * cost
        else:
            self._holding_cost = 0.0

        self._capital = self._coupon - self._holding_cost
        self._coupon_income.values[inow] = self._coupon
        self._holding_costs.values[inow] = self._holding_cost

    @property
    def coupon(self) -> float:
        """
        Returns
        -------
        float:
            coupon paid scaled by position in current step
        """
        if self.root.stale:
            self.root.update(self.root.now, None)
        return self._coupon

    @property
    def coupons(self) -> pd.DataFrame:
        """
        Returns
        -------
        pd.DataFrame:
            TimeSeries of coupon paid scaled by position in current step
        """
        if self.root.stale:
            self.root.update(self.root.now, None)
        return self._coupon_income.loc[: self.now]

    @property
    def holding_cost(self) -> float:
        """
        Returns
        -------
        float:
            holding cost scaled by position in current step
        """
        if self.root.stale:
            self.root.update(self.root.now, None)
        return self._holding_cost

    @property
    def holding_costs(self) -> pd.DataFrame:
        """
        Returns
        -------
        pd.DataFrame:
            TimeSeries of holding cost paid scaled by position in current step
        """
        if self.root.stale:
            self.root.update(self.root.now, None)
        return self._holding_costs.loc[: self.now]


class StrategyBase(Node):
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
        children: Union[list, dict] = None,
        parent: Node = None,
        PAR: int = 100,
    ):
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

        # default commission function
        self.commission_fn = self._dflt_comm_fn

        self._paper_trade = False
        self._positions = None
        self.bankrupt = False


class Algo(object):
    """
    Modularize strategy logic

    Algos are a function that receive the strategy as argument (referred to as target),
    and are expected to return a bool.

    Parameters
    ----------
        name: str
            name of algorithm
    """

    def __init__(self, name=None) -> None:
        self._name = name
        self.run_always = False

    @property
    def name(self):
        """
        Algo name.
        """
        if self._name is None:
            self._name = self.__class__.__name__
        return self._name

    def __call__(self, target):
        raise NotImplementedError("%s not implemented!" % self.name)


class AlgoStack(Algo):
    """
    Run multiple Algos until failure
    -> group logic set of Algos together.
    -> each Algo in stack is run
    -> execution stops if one Algo returns False

    Parameters
    ----------
    algos: list
        list of algos
    """

    def __init__(self, *algos) -> None:
        super().__init__()
        self.algos = algos

    def __call__(self, target: Strategy) -> bool:
        """
        Run all Algos in stack as long as True
        After False is returned, only run necessary Algos

        Parameters
        ----------
        target: Strategy
            Strategy

        Returns
        -------
        bool:
            ran all Algos in stack
        """
        res = True
        for algo in self.algos:
            if res:
                res = algo(target)
            elif algo.run_always:
                algo(target)
        return res
