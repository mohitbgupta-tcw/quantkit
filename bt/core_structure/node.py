from __future__ import division, annotations
from typing import Union
from copy import deepcopy
import numpy as np
import pandas as pd
import datetime


class Node(object):
    """
    Main building block in tree structure design
    --> main functionality of tree node

    Parameters
    ----------
    name: str
        node name
    parent: Node, optional
        parent Node
    children: dict | list, optional
        collection of children
    """

    def __init__(
        self, name: str, parent: Node = None, children: Union[list, dict] = None
    ) -> None:
        self.name = name
        self.integer_positions = True

        # children
        self.children = {}
        self._childrenv = []  # Shortcut to self.children.values()

        # add parent and children Nodes
        self._add_parent(parent=parent)
        self._add_children(children, dc=True)

        # set default value for now and inow
        self.now = 0
        self.inow = 0

        # helper vars
        self._price = 0
        self._value = 0
        self._notl_value = 0
        self._weight = 0
        self._capital = 0

        # flags
        self._issec = False
        self._fixed_income = False
        self._bidoffer_set = False
        self._bidoffer_paid = 0

    def __getitem__(self, key: str) -> Node:
        """
        Defines behavior for when an item is accessed using the notation self[key]
        Returns Node object of key

        Example
        -------
        strat["foo"]

        Parameters
        ----------
        key: str
            key of children

        Returns
        -------
        Node:
            Node of children identified by key
        """
        return self.children[key]

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

    def _add_children(self, children: Union[list, dict], dc: bool) -> None:
        """
        Add children to Node

        Parameters
        ----------
        children: list | dict
            collection of children
        dc: bool
            make deepcopy of child
        """
        # if at least 1 children is specified
        if children is not None:
            if isinstance(children, dict):
                children = list(children.values())

            for child in children:
                # TODO get rid of deepcopy logic
                if dc:
                    child = deepcopy(child)

                if child.name in self.children:
                    raise ValueError("Child %s already exists" % child)

                child.parent = self
                child._set_root(self.root)
                child.use_integer_positions(self.integer_positions)

                self.children[child.name] = child
                self._childrenv.append(child)

                # if strategy, turn on flag and add name to list
                if not child._issec:
                    self._strat_children.append(child.name)

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
        return self._value

    @property
    def notional_value(self) -> float:
        """
        Returns
        -------
        float:
            current notional (dollar) value of Node
        """
        return self._notl_value

    @property
    def weight(self) -> float:
        """
        Returns
        -------
        float:
            current weight of Node with respect to parent
        """
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

    def pre_settlement_update(self, date: datetime.date, inow: int = None) -> None:
        """
        Update Node with latest date, make Node ready for Rebalancing logic

        Parameters
        ----------
        date: datetime.date
            current date
        inow: int, optional
            current integer position
        """
        raise NotImplementedError()

    def post_settlement_update(self):
        """
        Update Node after Rebalancing logic
        """
        raise NotImplementedError()

    def adjust(self, amount: float, flow: bool = True) -> None:
        """
        Adjust Node value by amount

        Parameters
        ----------
        amount: float
            adjustment amount
        flow: bool
            adjustment is flow (capital injections)
            -> no impact on performance
            -> should not be reflected in returns
            adjustment is non-flow (comission, dividend)
            -> impact performance
        """
        raise NotImplementedError()

    def allocate(self, amount: float) -> None:
        """
        Allocate capital to Node

        Parameters
        ----------
        amount: float
            allocation amount
        """
        raise NotImplementedError()

    @property
    def members(self) -> list:
        """
        Node members - include current node as well as Node's children

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
    def full_name(self) -> str:
        """
        Returns
        -------
        str:
            full name of Node in form root.name > ... > parent.name > node.name
        """
        if self.parent == self:
            return self.name
        else:
            return f"{self.parent.full_name}>{self.name}"

    def __repr__(self) -> str:
        """
        Returns
        -------
        str:
            representative name of Node in form <Node type Node.name>
        """
        return f"<{self.__class__.__name__} {self.full_name}>"
