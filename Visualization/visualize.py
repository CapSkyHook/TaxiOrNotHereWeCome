import pandas as pd
import glob,os
import csv

#the data is copied to the local machine
os.chdir("./to_visualization")

print("Starting ...")
df = pd.DataFrame()
for i in range(0,101):
    with open("part-%05d"%i) as csvfile:
        for line in csvfile:
            lists = []
            line = line.strip("(())\n").strip(")")
            #print(line[:-3])
            #line = line[:-3]+line[-2:]
            l = line.split(",")
            l[1] = line.split(",")[1][:-1]
            #print(l)
            #print(float(line.split(",")[0]))
            #df = df.append(pd.DataFrame(line.split(","),columns= ["lats","long","count"]),ignore_index=True)
            #print(re.sub(r" ?\([^)]+\)", "", line))
            lists.append(l)
            df = df.append(pd.DataFrame(lists))
la=list(df[0].values)
lo=list(df[1].values)
la = [float(l) for l in la]
lo = [float(l) for l in lo]
data = zip(la,lo)
las,lons=zip(*data)

from gmplot import gmplot
gmap = gmplot.GoogleMapPlotter.from_geocode("New York City")

gmap.scatter(las,lons, size=10, marker=False)
gmap.draw("test_nyc_map.html")
     
print("Done!")
 

