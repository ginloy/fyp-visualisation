from typing import Dict, List, Sequence, Tuple, Any, Type
from dash import Dash, html, callback, dash_table, dcc, Input, Output, no_update
import dash_bootstrap_components as dbc
import dash_ag_grid as dag
from dash_bootstrap_templates import load_figure_template
import polars as pl
import plotly.express as px
import plotly.graph_objs as go
from functools import cache
import polars.selectors as cs

from globals import DF


def selectors() -> dbc.Row:
    fields = ["id1", "config", "accel_pos"]
    col_lst: List[dbc.Col] = []
    for field in fields:
        series = DF.select(field).unique().collect().to_series().sort()
        options = series.to_list()
        col_lst.append(
            dbc.Col(
                [
                    dbc.Label(field),
                    dcc.Dropdown(
                        options,
                        id=f"selector-{field}",
                        searchable=True,
                        persistence=True,
                        clearable=False if field == "id1" else True,
                    ),
                ],
                width=3,
            )
        )
    return dbc.Row(col_lst, class_name="px mt-2")


def get_graphs(config: str, accel_pos: str, id: str) -> List[Tuple[str, go.Figure]]:
    df = DF
    figs: List[Tuple[str, go.Figure]] = []
    if config is not None:
        df = df.filter(pl.col("config") == config)
    if accel_pos is not None:
        df = df.filter(pl.col("accel_pos") == accel_pos)
    if id is not None:
        df = df.filter(pl.col("id1") == id)

    dfs = df.collect().partition_by("id1", as_dict=True)
    for grp, df in dfs.items():
        fig = px.line(
            df,
            x="x",
            y="data_mag",
            color="axis",
            facet_col="config",
            facet_row="accel_pos",
        )
        figs.append((grp[0], fig))  # type:ignore
    return figs


def get_data(config: str, accel_pos: str, id: str) -> Sequence[Dict[Any, Any]]:
    df = DF
    if config is not None:
        df = df.filter(pl.col("config") == config)
    if accel_pos is not None:
        df = df.filter(pl.col("accel_pos") == accel_pos)
    if id is not None:
        df = df.filter(pl.col("id1") == id)
    return df.collect().to_dicts()


@callback(
    Output("tab-contents", "children"),
    Input("selector-config", "value"),
    Input("selector-accel_pos", "value"),
    Input("selector-id1", "value"),
    Input("tabs", "value"),
    running=[
        (
            Output("tab-contents", "children"),
            dbc.Row(
                dbc.Spinner(spinner_style={"height": "200px", "width": "200px"}),
                align="center",
                justify="center",
                className="h-100",
            ),
            [],
        )
    ],
)
@cache
def update_graphs(config, accel_pos, id, tab):
    print("update_Graphs")
    if tab == "graphs-tab":
        return [
            dbc.Row(
                dbc.Col(
                    [dbc.Label(id), dcc.Graph(figure=fig, style=dict(height="70vh"))]
                )
            )
            for id, fig in get_graphs(config, accel_pos, id)
        ]
    elif tab == "data-tab":
        number_cols = ["x", "data_real", "data_imag", "data_mag", "data_phase"]
        columnDefs = [
            {"field": i, "filter": "agNumberColumnFilter" if i in number_cols else True}
            for i in DF.collect_schema().names()
        ]
        return [
            dag.AgGrid(
                persistence=True,
                id="data-grid",
                rowModelType="infinite",
                columnDefs=columnDefs,
                defaultColDef=dict(floatingFilter=True, filter=True, sorting=True),
                dashGridOptions=dict(pagination=True),
                style=dict(height="100%"),
                persisted_props=["filterModel"],
            ),
        ]


@callback(Output("data-grid", "getRowsResponse"), Input("data-grid", "getRowsRequest"))
def infinite_scroll(request):
    if request is None:
        return no_update
    df = DF
    expr = pl.lit(True)
    if request["filterModel"]:
        filters = request["filterModel"]
        for k, filter in filters.items():
            colType = df.select(k).collect_schema().dtypes()[0].to_python()
            print(k, filter)
            if "operator" in filter:
                if filter["operator"] == "AND":
                    expr &= filter_df(expr, filter["condition1"], k, colType)
                    expr &= filter_df(expr, filter["condition2"], k, colType)
                else:
                    expr1 = filter_df(pl.lit(True), filter["condition1"], k, colType)
                    expr2 = filter_df(pl.lit(True), filter["condition2"], k, colType)
                    expr &= expr1 | expr2
            else:
                expr &= filter_df(expr, filter, k, colType)
    df = df.filter(expr)
    if request["sortModel"]:
        sorting = []
        desc = []
        for sort in request["sortModel"]:
            sorting.append(sort["colId"])
            if sort["sort"] == "asc":
                desc.append(False)
            else:
                desc.append(True)
        df = df.sort(by=sorting, descending=desc)

    start, end = request["startRow"], request["endRow"]
    temp = dict(
        rowData=df.slice(start, end - start).collect().to_dicts(),
        rowCount=df.select(pl.len()).collect().item(),
    )
    return temp


def filter_df(expr: pl.Expr, data, col: str, colType: Type):
    exprStr = pl.col(col).cast(pl.String).str
    exprRange = pl.col(col)
    ftype = data["type"]
    crit1 = ""
    crit2 = ""
    if "filter" in data:
        crit1 = data["filter"]
    if "filterTo" in data:
        crit2 = data["filterTo"]
    if ftype == "contains":
        expr &= exprStr.contains(crit1)
    elif ftype == "notContains":
        expr &= ~exprStr.contains(crit1)
    elif ftype == "startsWith":
        expr &= exprStr.starts_with(crit1)
    elif ftype == "notStartsWith":
        expr &= ~exprStr.starts_with(crit1)
    elif ftype == "endsWith":
        expr &= exprStr.ends_with(crit1)
    elif ftype == "notEndsWith":
        expr &= ~exprStr.ends_with(crit1)
    elif ftype == "inRange":
        expr &= exprRange.is_between(colType(crit1), colType(crit2))  # type:ignore
    elif ftype == "blank":
        expr &= exprRange.is_null()
    elif ftype == "notBlank":
        expr &= exprRange.is_not_null()
    elif ftype == "equals":
        expr &= exprRange == colType(crit1)
    elif ftype == "notEqual":
        expr &= exprRange != colType(crit1)
    elif ftype == "greaterThan":
        expr &= exprRange > colType(crit1)
    elif ftype == "greaterThanOrEqual":
        expr &= exprRange >= colType(crit1)
    elif ftype == "lessThan":
        expr &= exprRange < colType(crit1)
    elif ftype == "lessThanOrEqual":
        expr &= exprRange <= colType(crit1)
    return expr


# @callback(
#     Output("data-table", "getRowsResponse"), Input("data-table", "getRowsRequest")
# )
# def infinite_scroll(request):
#     if request is None:
#         return no_update
#     df = DF
#     if request["filterModel"]:
#         filters = request["filterModel"]
#         for k, filter in filters.items():
#             if "operator" in filter:
#                 if filter["operator"] == "AND":
#                     pass


dbc_css = "https://cdn.jsdelivr.net/gh/AnnMarieW/dash-bootstrap-templates/dbc.min.css"
app = Dash(
    __name__,
    external_stylesheets=[dbc.themes.BOOTSTRAP, dbc_css],
    # background_callback_manager=BG_CALLBACK_MANAGER,
    compress=True,
    suppress_callback_exceptions=True,
)
server = app.server
load_figure_template("bootstrap_dark")

types = DF.select("id1").unique().collect().to_series().sort().to_list()

app.layout = dbc.Container(
    fluid=True,
    className="dbc dbc-ag-grid d-flex flex-column vh-100",
    children=[
        dbc.Row(
            class_name="mt-2",
            children=[
                dbc.Col(
                    children=dcc.Tabs(
                        persistence=True,
                        id="tabs",
                        value="graphs-tab",
                        children=[
                            dcc.Tab(
                                label="Graphs",
                                value="graphs-tab",
                                children=selectors(),
                            ),
                            dcc.Tab(label="Raw Data", value="data-tab"),
                        ],
                    ),
                ),
            ],
        ),
        dbc.Row(
            className="flex-grow-1 overflow-auto mt-2",
            # align="center",
            # justify="center",
            children=dbc.Col(
                id="tab-contents",
                className="h-100",
            ),
        ),
        # dbc.Row(
        #     [
        #         dag.AgGrid(
        #             rowData=temp.to_dicts(),  # type: ignore
        #             columnDefs=[
        #                 {"field": i, "filter": True, "floatingFilter": True}
        #                 for i in temp.columns
        #             ],
        #             id="main-table",
        #             dashGridOptions=dict(pagination=True),
        #         ),
        #     ]
        # ),
        # html.Div([], id="graph-tab"),
    ],
)


if __name__ == "__main__":
    app.run(debug=True)
