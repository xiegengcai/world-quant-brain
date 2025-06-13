# %%
import json
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from pyvis.network import Network
import pandas as pd
from tool.cache_tool import CacheManager

sgr = SessionManager()


df = pd.read_pickle("result/available_alphas.pkl")

cm = CacheManager(max_size=1000, default_expire_time=10)


@cm.cache(expire_time=3600 * 24 * 30)
def fetch_pnl(alpha_id):
    return get_alpha_pnl(sgr, alpha_id).set_index("Date")


with ThreadPoolExecutor(max_workers=3) as executor:
    results = executor.map(fetch_pnl, df["id"].tolist())
alpha_pnls = pd.concat(list(results), axis=1)
alpha_pnls.sort_index(inplace=True)

# %%
alpha_pnls_delta = alpha_pnls - alpha_pnls.ffill().shift(1)
alpha_pnls_delta = alpha_pnls_delta[
    pd.to_datetime(alpha_pnls_delta.index)
    > pd.to_datetime(alpha_pnls_delta.index).max() - pd.DateOffset(years=4)
]


# %%
import pickle

id_2_label = df.set_index("id").to_dict(orient="index")

# %%
corr_relations = alpha_pnls_delta.corr()
corr_relations.to_pickle("corr/corr_relations.pkl")

# %%
# for index, row in corr_relations.round(2).iterrows():
#     print(f"\n行 {index}:")
#     for col_name in corr_relations.columns:
#         print(f"{col_name}: {row[col_name]}")


# %%

net = Network(notebook=False, height="600px", width="100%")
net.set_options(
    json.dumps(
        {
            "nodes": {
                "size": 24,
                "font": {"background": "rgba(255,255,255,0.7)"},
                "borderWidth": 0,
                "color": "#1565C0",
            },
            "physics": {
                "solver": "repulsion",
                "repulsion": {
                    "centralGravity": 0.2,
                    "springLength": 200,
                    "springConstant": 0.05,
                    "nodeDistance": 100,
                    "damping": 0.1,
                },
            },
        }
    )
)


def add_node(row):
    params = id_2_label[row.name]
    sharpe = params["sharpe"]
    fitness = params["fitness"]
    turnover = params["turnover"]
    returns = params["returns"]
    label = f"id={row.name}\n{sharpe=}\n{fitness=}\n{turnover=}\n{returns=}"
    net.add_node(
        row.name,
        label=label,
        # font={"background": "rgba(255,255,255,0.7)"},
        # shape="circle",
        # color="#1565C0",  # 节点颜色
        # font={"color": "white", "size": 20},
    )


# 添加节点
df.set_index("id").apply(
    add_node,
    axis=1,
)
# # 添加边 (u, v, weight)
for index, row in corr_relations.round(2).iterrows():
    for col_name in corr_relations.columns:
        if col_name != index:
            if row[col_name] > 0.7:
                # G.add_weighted_edges_from([(index,col_name,row[col_name])])
                net.add_edge(index, col_name, title=row[col_name])
                # print(row[col_name])
# net.options.physics.barnesHut.springLength = 100
# net.options.physics.barnesHut.springConstant = 0.04
# net.options.physics.barnesHut.damping = 0.09
# net.repulsion(node_distance=100, spring_length=100)  # 调节排斥力
# net.show("graph.html")  # 内嵌显示
net.write_html("graph.html", notebook=False)
