import pandas as pd
import numpy as np
import math
import datetime
import quantkit.bt.core_structure.node as node
import quantkit.bt.util.util_functions as util_functions


class SecurityBase(node.Node):
    """
    Security Node:
        - models asset that can be bought or sold
        - define security within tree
        - security has no children

    Parameters
    ----------
    name: str
        security name
    multiplier: float, optional
        security multiplier - typically used for derivatives or to trade in lots
    """

    def __init__(self, name: str, multiplier: float = 1) -> None:
        super().__init__(name, parent=None, children=None)
        self.multiplier = multiplier

        # opt
        self._position = 0
        self._last_pos = 0
        self._issec = True
        self._outlay = 0
        self._bidoffer = 0
        self._accrued_interest = 0

    @property
    def price(self) -> float:
        """
        Returns
        -------
        float:
            current price of security
        """
        return self._price

    @property
    def prices(self) -> pd.DataFrame:
        """
        Returns
        -------
        pd.DataFrame:
            TimeSeries of security's price
        """
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
        return self._outlays.loc[: self.now]

    @property
    def bidoffer(self) -> float:
        """
        Returns
        -------
        float:
            current bid/offer spread
        """
        return self._bidoffer

    @property
    def bidoffers(self) -> pd.DataFrame:
        """
        Returns
        -------
        pd.DataFrame:
            TimeSeries of bid/offer spread on transactions
        """
        if self._bidoffer_set:
            return self._bidoffers.loc[: self.now]
        else:
            raise Exception(
                "Bid/offer accounting not turned on: "
                '"bidoffer" argument not provided during setup'
            )

    @property
    def bidoffer_paid(self) -> float:
        """
        Returns
        -------
        float:
            bid/offer spread paid on transactions in current step
        """
        return self._bidoffer_paid

    @property
    def bidoffers_paid(self) -> pd.DataFrame:
        """
        Returns
        -------
        pd.DataFrame:
            TimeSeries of bid/offer spread paid on transactions
        """
        if self._bidoffer_set:
            return self._bidoffers_paid.loc[: self.now]
        else:
            raise Exception(
                "Bid/offer accounting not turned on: "
                '"bidoffer" argument not provided during setup'
            )

    def setup(self, prices: pd.DataFrame, **kwargs) -> None:
        """
        Setup Security

        Parameters
        ----------
        prices: pd.DataFrame
            DataFrame of prices for securities in universe
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
        if "bidoffer" in kwargs:
            self._bidoffer_set = True

            if self.name in kwargs["bidoffer"].columns:
                bidoffers = kwargs["bidoffer"][self.name]
                if bidoffers.index.equals(prices.index):
                    self._bidoffers = bidoffers
                    self.data["bidoffer"] = self._bidoffers
                else:
                    raise ValueError("Index of bidoffer must match price data")
            else:
                self.data["bidoffer"] = 0.0
                self._bidoffers = self.data["bidoffer"]

            self.data["bidoffer_paid"] = 0.0
            self._bidoffers_paid = self.data["bidoffer_paid"]

    def pre_settlement_update(self, date: datetime.date, inow: int) -> None:
        """
        Update security before running Rebalance()
        Reset:
            - date
            - prices
            - values

        Parameters
        ----------
        date: datetime.date
            current date
        inow: int
            current location in price DataFrame
        """
        # update now
        self.now = date
        self.inow = inow

        # update price
        self._price = self._prices.values[inow]

        # update bid/offer
        if self._bidoffer_set:
            self._bidoffer = self._bidoffers.values[inow]
            self._bidoffer_paid = 0.0

        # update security capital
        if np.isnan(self._price):
            if util_functions.is_zero(self._position):
                self._value = 0
                self._notl_value = 0
            else:
                raise Exception(
                    "Position is open (non-zero: %s) and latest price is NaN "
                    "for security %s on %s. Cannot update node value."
                    % (self._position, self.name, date)
                )
        else:
            self._value = self._position * self._price * self.multiplier
            self._notl_value = self._value

        self._values.values[inow] = self._value
        self._notl_values.values[inow] = self._notl_value

    def post_settlement_update(self) -> None:
        """
        Update security after running Rebalance()

        Set:
            - positions
            - outlay
            - bidoffers
        """
        # update position size
        self._positions.values[self.inow] = self._position
        self._last_pos = self._position

        # save outlay to outlays
        if self._outlay != 0:
            self._outlays.values[self.inow] += self._outlay
            self._outlay = 0

        # save bidoffers
        if self._bidoffer_set:
            self._bidoffers_paid.values[self.inow] = self._bidoffer_paid

    def allocate(self, amount: float) -> None:
        """
        Allocate capital (dollar amount) to security -> calculate amount of shares to sell/ buy
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
            dollar amount of adjustment
        """
        if util_functions.is_zero(amount):
            return

        if util_functions.is_zero(self._price) or np.isnan(self._price):
            raise Exception(
                f"""Cannot allocate capital to "{self.name}" because price is {self._price} as of {self.now}"""
            )

        # closing out
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

            # if full outlay > amount, we must decrease the magnitude of `q`
            full_outlay, _, _, _ = self.outlay(q)
            i = 0
            last_q = q
            last_amount_short = full_outlay - amount
            while not np.isclose(full_outlay, amount, rtol=1e-16) and q != 0:
                dq_wout_considering_tx_costs = last_amount_short / (
                    self._price * self.multiplier
                )
                q -= dq_wout_considering_tx_costs

                if self.integer_positions:
                    q = math.floor(q)

                full_outlay, _, _, _ = self.outlay(q)

                if self.integer_positions:
                    full_outlay_of_1_more, _, _, _ = self.outlay(q + 1)

                    if full_outlay < amount and full_outlay_of_1_more > amount:
                        break

                i = i + 1
                if i > 1e4:
                    raise Exception("Potentially infinite loop detected.")

                if self.integer_positions and last_q == q:
                    raise Exception(
                        "Newton Method like root search for quantity is stuck!"
                    )
                last_q = q

                if np.abs(full_outlay - amount) > np.abs(last_amount_short):
                    raise Exception(
                        "Last amount short got bigger since last iteration."
                    )
                last_amount_short = full_outlay - amount
        self.transact(q)

    def transact(self, q: float, price: float = None) -> None:
        """
        Transacts security --> buy/sell the security for a given quantity
            - commission will be calculated based on the parent's commission function and quantity and price passed in
            - any remaining capital will be passed back up to parent as adjustment

        Parameters
        ----------
        q: float
            quantity of shares to buy/ sell
        price: float, optional
            transaction price
        """
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
        self.parent.adjust(-full_outlay, flow=False, fee=fee)

        # store outlay for future reference
        self._outlay += outlay
        self._bidoffer_paid += bidoffer

    def commission(self, q: float, p: float) -> float:
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
            fee = self.commission(q, p * self.multiplier)
            bidoffer = q * (p - self._price) * self.multiplier

        outlay = q * self._price * self.multiplier + bidoffer

        return outlay + fee, outlay, fee, bidoffer

    def run(self) -> None:
        """
        Securities have nothing to do on run
        """
        pass


class Security(SecurityBase):
    """
    Wrapper around SecurityBase

    Parameters
    ----------
    name: str
        security name
    multiplier: float, optional
        security multiplier - typically used for derivatives or to trade in lots
    """

    pass


class FixedIncomeSecurity(SecurityBase):
    """
    A Fixed Income Security

    Parameters
    ----------
    name: str
        security name
    multiplier: float, optional
        security multiplier - typically used for derivatives or to trade in lots
    """

    def __init__(self, name: str, multiplier: float = 1) -> None:
        super().__init__(name, multiplier)
        self._fixed_income = True

    def pre_settlement_update(self, date: datetime.date, inow: int) -> None:
        """
        Update security before running Rebalance()
        Reset:
            - date
            - prices
            - values

        Parameters
        ----------
        date: datetime.date
            current date
        inow: int
            current location in price DataFrame
        """
        super().pre_settlement_update(date, inow)

        # For fixed income securities (bonds, swaps), notional value is position size, not value!
        self._notl_value = self._position

    def post_settlement_update(self) -> None:
        """
        Update security after running Rebalance()

        Set:
            - positions
            - outlay
            - bidoffers
        """
        super().post_settlement_update()

        # For fixed income securities (bonds, swaps), notional value is position size, not value!
        self._notl_value = self._position
        self._notl_values.values[self.inow] = self._notl_value


class CouponPayingSecurity(FixedIncomeSecurity):
    """
    CouponPayingSecurity represents a coupon-paying security, where coupon payments (possibly irregular) adjust
    the capital of the parent. Coupons and costs must be passed in during setup.

    Parameters
    ----------
    name: str
        security name
    multiplier: float, optional
        security multiplier - typically used for derivatives or to trade in lots
    """

    def __init__(self, name: str, multiplier: float = 1) -> None:
        super().__init__(name, multiplier)
        self._coupon = 0
        self._holding_cost = 0

    def setup(self, prices: pd.DataFrame, **kwargs) -> None:
        """
        Setup Security

        Parameters
        ----------
        prices: pd.DataFrame
            DataFrame of prices for securities in universe
        """
        super().setup(prices, **kwargs)

        # Handle coupons
        if "coupons" not in kwargs:
            raise Exception(
                '"coupons" must be passed to setup for a CouponPayingSecurity'
            )

        if self.name in kwargs["coupons"].columns:
            self._coupons = kwargs["coupons"][self.name]
        else:
            raise ValueError("Index of coupons must match universe data")

        if not self._coupons.index.equals(prices.index):
            raise ValueError("Index of coupons must match universe data")

        if self.name in kwargs["payment_schedule"].columns:
            self._payment_schedule = kwargs["payment_schedule"][self.name]
        else:
            raise ValueError("Index of payment_schedule must match universe data")

        if not self._payment_schedule.index.equals(prices.index):
            raise ValueError("Index of payment_schedule must match universe data")

        try:
            self._cost_long = kwargs["cost_long"][self.name]
        except KeyError:
            self._cost_long = None
        try:
            self._cost_short = kwargs["cost_short"][self.name]
        except KeyError:
            self._cost_short = None

        self.data["coupon"] = 0.0
        self.data["holding_cost"] = 0.0
        self._coupon_income = self.data["coupon"]
        self._holding_costs = self.data["holding_cost"]

    def pre_settlement_update(self, now: datetime.date, inow: int) -> None:
        """
        Update security before running Rebalance()
        Reset:
            - date
            - prices
            - values

        Parameters
        ----------
        date: datetime.date
            current date
        inow: int
            current location in price DataFrame
        """
        super().pre_settlement_update(now, inow)

        coupon = self._coupons.values[self.inow]

        if np.isnan(coupon):
            if util_functions.is_zero(self._position):
                self._coupon = 0.0
            else:
                raise Exception(
                    f"""Position is open and latest coupon is NaN for security {self.name} on {self.now}."""
                )
        else:
            self._coupon = self._position * coupon

        if self._position > 0 and self._cost_long is not None:
            cost = self._cost_long.values[self.inow]
            self._holding_cost = self._position * cost
        elif self._position < 0 and self._cost_short is not None:
            cost = self._cost_short.values[self.inow]
            self._holding_cost = -self._position * cost
        else:
            self._holding_cost = 0.0

        self._accrued_interest += self._coupon

        if self._payment_schedule.values[self.inow] == 1:
            self._capital += self._accrued_interest
            self._coupon_income.values[self.inow] = self._accrued_interest
            self._accrued_interest = 0

        self._capital -= self._holding_cost
        self._holding_costs.values[self.inow] = self._holding_cost

    @property
    def coupon(self) -> float:
        """
        Returns
        -------
        float:
            coupon payment (scaled by position) of current period
        """
        return self._coupon

    @property
    def coupons(self) -> pd.DataFrame:
        """
        Returns
        -------
        pd.DataFrame
            TimeSeries of coupons paid (scaled by position)
        """
        return self._coupon_income.loc[: self.now]

    @property
    def holding_cost(self) -> float:
        """
        Returns
        -------
        float:
            holding cost (scaled by position) of current period
        """
        return self._holding_cost

    @property
    def holding_costs(self) -> pd.DataFrame:
        """
        Returns
        -------
        pd.DataFrame:
            TimeSeries of coupons paid (scaled by position)
        """
        return self._holding_costs.loc[: self.now]


class HedgeSecurity(SecurityBase):
    """
    HedgeSecurity is a SecurityBase where the notional value is set to zero, and thus
    does not count towards the notional value of the strategy. It is intended for use
    in fixed income strategies.

    Parameters
    ----------
    name: str
        security name
    multiplier: float, optional
        security multiplier - typically used for derivatives or to trade in lots
    """

    def pre_settlement_update(self, date: datetime.date, inow: int = None) -> None:
        """
        Update security before running Rebalance()
        Reset:
            - date
            - prices
            - values

        Parameters
        ----------
        date: datetime.date
            current date
        inow: int
            current location in price DataFrame
        """
        super().pre_settlement_update(date, inow)
        self._notl_value = 0.0
        self._notl_values.values.fill(0.0)

    def post_settlement_update(self) -> None:
        """
        Update security after running Rebalance()

        Set:
            - positions
            - outlay
            - bidoffers
        """
        super().post_settlement_update()
        self._notl_value = 0.0
        self._notl_values.values.fill(0.0)


class CouponPayingHedgeSecurity(CouponPayingSecurity):
    """
    CouponPayingHedgeSecurity is a CouponPayingSecurity where the notional value is set to zero, and thus
    does not count towards the notional value of the strategy. It is intended for use
    in fixed income strategies.

    Parameters
    ----------
    name: str
        security name
    multiplier: float, optional
        security multiplier - typically used for derivatives or to trade in lots
    """

    def pre_settlement_update(self, date: datetime.date, inow: int = None) -> None:
        """
        Update security before running Rebalance()
        Reset:
            - date
            - prices
            - values

        Parameters
        ----------
        date: datetime.date
            current date
        inow: int
            current location in price DataFrame
        """
        super().pre_settlement_update(date, inow)
        self._notl_value = 0.0
        self._notl_values.values.fill(0.0)

    def post_settlement_update(self) -> None:
        """
        Update security after running Rebalance()

        Set:
            - positions
            - outlay
            - bidoffers
        """
        super().post_settlement_update()
        self._notl_value = 0.0
        self._notl_values.values.fill(0.0)
