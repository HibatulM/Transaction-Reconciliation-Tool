import pandas as pd

print("-----------Starting Reconciliation with data from system A and B-----------")

data_a = pd.read_csv("data/system_A.csv")
data_b = pd.read_csv("data/system_B.csv")

mergedDataSet = data_a.merge(data_b,
                        on="transaction_id",
                        how="outer",
                        suffixes=("_a", "_b"),
                        indicator=True
                )
data_a_missing = mergedDataSet[mergedDataSet["_merge"] == "right_only"]
data_b_missing = mergedDataSet[mergedDataSet["_merge"] == "left_only"]

missing_num = mergedDataSet[
    (mergedDataSet["_merge"] == "both") & 
    (mergedDataSet["amount_a"] != mergedDataSet["amount_b"])
]


print(f"Total records in system A: {len(data_a)}")
print(f"Total records in system B: {len(data_b)}")

#Output the missing data and any discrapancies in data
print(f"Missing data in system A: {len(data_a_missing)}")
print(f"Missing data in system B: {len(data_b_missing)}")

#Output how much was missing 
print(f"Amount mismatches: {len(missing_num)}\n")

#Printing the actual discrepancies 
if not data_a_missing.empty:
    print("Transactions with discrepancies:")
    print(data_a_missing[['transaction_id', 'amount_a', 'amount_b']])
else:
    print("No Discrepancies found in System A")
    print()

if not data_b_missing.empty:
    print("Transaction missing in System B:")
    print(data_b_missing[['transaction_id', 'amount_a']])
else:
    print("No missing data in System B")
