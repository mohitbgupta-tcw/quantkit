import pandas as pd
from dash import html


class Table(object):
    """
    Create a dash definition of an HTML table for a Pandas dataframe

    Parameters
    ----------
    df: pd.DataFrame
        DataFrame to be displayed in table
    id: str, optional
        id of html table
    show_vertical_lines: bool, optional
        show vertical lines between columns
    show_header: bool, optional
        show table header
    add_vertical_column: str, optional
        text of vertical column to be added in right column
    """

    def __init__(
        self,
        df: pd.DataFrame,
        id: str = "",
        show_vertical_lines: bool = False,
        show_header: bool = False,
        add_vertical_column: str = None,
    ):
        self.data = df
        self.id = id
        self.show_vertical_lines = show_vertical_lines
        self.show_header = show_header
        self.add_vertical_column = add_vertical_column

    def create_table(self) -> html.Table:
        """
        Create table

        Returns
        -------
        html.Table
            html table
        """
        table = []

        # classes with and without vertical lines
        if self.show_vertical_lines:
            class_td = "table-element vertical"
            class_table = "multi-table"
            class_header = "vertical"
        else:
            class_td = "table-element"
            class_table = ""
            class_header = ""

        # add header row
        if self.show_header:
            table.append(self.add_header_row(class_header))

        # add rows
        if self.add_vertical_column:
            for index, row in self.data.iterrows():
                html_row = []
                for i in range(len(row)):
                    html_row.append(html.Td([row[i]], className=class_td))
                if index == 0:
                    html_row.append(
                        html.Td(
                            [f"{self.add_vertical_column}"],
                            rowSpan=f"{len(self.data)}",
                            className="vert-text",
                        )
                    )
                table.append(html.Tr(html_row))

        else:
            for index, row in self.data.iterrows():
                html_row = []
                for i in range(len(row)):
                    html_row.append(html.Td([row[i]], className=class_td))
                table.append(html.Tr(html_row))
        return html.Table(table, className=class_table, id=self.id)

    def add_header_row(self, class_header: str) -> html.Tr:
        """
        Add column names as Th elements in row

        Parameters
        ----------
        class_header: str
            class for header cells

        Returns
        html.Tr
            table row of column names
        """
        # add header row
        header_row = []
        for i, col in enumerate(self.data.columns):
            header_row.append(html.Th(col, className=class_header))
        return html.Tr(header_row, className="table-header")
