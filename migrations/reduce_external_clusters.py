import csv
print("Starting reduce external clusters")

old_file = "beebop/resources/GPS_v6_external_clusters_ORIGINAL.csv"
new_file = "beebop/resources/GPS_v6_external_clusters.csv"

with open(old_file) as f_old:
    reader = csv.reader(f_old, delimiter=',')
    with open(new_file, "w") as f_new:
        writer = csv.writer(f_new, delimiter=",")

        headers = next(reader)
        print("Found {} headers".format(len(headers)))
        writer.writerow(headers)
        for old_row in reader:
            new_row = [old_row[0]]
            if len(old_row) > 1:
                clusters = old_row[1].split(";")
                numeric_clusters = [int(x) for x in clusters]
                numeric_clusters.sort()
                new_row.append(numeric_clusters[0])
            writer.writerow(new_row)