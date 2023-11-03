import pandas as pd

class zipCount:
    def __init__(self, zip, count):
        self.zip = zip
        self.count = count

class combinedKey:
    def __init__(self, key, zipList):
        self.key = key
        self.zipList = zipList

class transactKey:
    def __init__(self, key):
        self.key = key
        self.count = 1

processedKeys = []
master = []

transactionsKeys = []
transactMaster = []

path_transactions = "C:\\Users\\patri\\Downloads\\transactionsZIP.csv"
path_creditNote = "C:\\Users\\patri\\Downloads\\salescreditnoteZIP.csv"

transactions = pd.read_csv(path_transactions)
creditNote = pd.read_csv(path_creditNote)

duplicateCount = 0

for index, row in creditNote.iterrows():
    currentKey = creditNote.loc[index, "CombinedKey"]
    currentZip = creditNote.loc[index, "zip_area__c"]
    # print(currentKey)

    if currentKey not in processedKeys:
        processedKeys.append(currentKey)

        zc = zipCount(currentZip, 1)
        ck = combinedKey(currentKey, [zc])
        master.append(ck)
    else:
        for i in master:
            if i.key == currentKey:
                zipExists = False

                for x in i.zipList:
                    if x.zip == currentZip:
                        x.count += 1
                        zipExists = True
                
                if zipExists == False:
                    i.zipList.append(zc)

                break
                    
        duplicateCount += 1

# Error Checking - Comparing total counts of CombinedKeys in Transaction Lines to Credit Note
for index, row in transactions.iterrows():
    currentKey = transactions.loc[index, "CombinedKey"]
    
    if currentKey not in transactionsKeys:
        transactionsKeys.append(currentKey)

        tk = transactKey(currentKey)
        transactMaster.append(tk)
    else:
        for i in transactMaster:
            if i.key == currentKey:
                i.count += 1
                break

with open('output.txt', 'w') as f:
    f.write("---Credit Note Combination Key + Zip/Zip Counts---" + "\n")
    for i in master:
        f.write(i.key + "\n")

        for x in i.zipList:
            f.write("Zip: " + x.zip + "\n")
            f.write("Count: " + str(x.count) + "\n")
        
        f.write("\n")

    f.write("---Combination Key Checking---\n\n")
    mismatch = 0
    invalid = 0
    for i in master:
        total = 0

        for x in i.zipList:
            total += x.count

        for y in transactMaster:
            if i.key == y.key:
                if total != y.count:
                    f.write(i.key + "\n")
                    f.write("Credit Note count: " + str(total) + "\n")
                    f.write("Transaction Line count: " + str(y.count) + "\n")
                    mismatch += 1

                    if(total > y.count):
                        f.write("Valid: TRUE\n\n")
                    else:
                        f.write("Valid: FALSE\n\n")
                        invalid += 1
                else:
                    continue
    
    f.write("Total Unique Combination Keys - Credit Note: " + str(len(master)) + "\n")
    f.write("Total Duplicate Combination Keys in Credit Note: " + str(duplicateCount) + "\n")
    f.write("Total Combination Key Mismatches: " + str(mismatch) + "\n")
    f.write("Total Invalid CK Count - Credit Note < Transaction Lines: " + str(invalid))