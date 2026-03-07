import pandas as pd 
# concepts in the ones; mostly should do what's right
df = pd.read_parquet("input/MRREL.parquet")
cset = {"C0030842",
"C0729074",
"C0031006",
"C0003392",
"C0013595",
"C0020523",
"C0020456",
"C0011849",
"C0430456",
"C1623258",
"C0011603",
"C0013595"}


subset = df[(df.CUI1.isin(cset)) & (df.CUI2.isin(cset)) & (df.REL == "RN")]
subset.to_csv("leaky_rels.csv", index=False)