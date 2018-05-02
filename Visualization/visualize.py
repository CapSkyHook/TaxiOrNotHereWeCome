import pandas as pd
import glob,os
import csv
from tqdm import tqdm

#the data is copied to the local machine
#plot heatmap
os.chdir("./to_visualization")

print("Creating HTML file for heatmap ...")
df = pd.DataFrame()
for i in tqdm(range(0,1763)):
    df1 = pd.read_csv("part-%05d"%i, header=None)
    df1[0] = df1[0].apply( lambda x: float(x.strip("(())\n").strip(")")))
    df1[1] = df1[1].apply( lambda x: float(x.strip("(())\n").strip(")")))
    df1[2] = df1[2].apply( lambda x: float(x.strip("(())\n").strip(")")))
    df = pd.concat([df1,df])
    
la=list(df[0].values)
lo=list(df[1].values)
la = [float(l) for l in la]
lo = [float(l) for l in lo]
data = zip(la,lo)
las,lons=zip(*data)

from gmplot import gmplot
gmap = gmplot.GoogleMapPlotter.from_geocode("New York City")
gmap.heatmap(las,lons)
gmap.draw("heatmap.html")
     
print("Done!")

#plot clusters and centers
os.chdir("./cluster_centers")
print("Creating HTML file for clusters and stations ...")

df = pd.DataFrame()
for i in range(0,223):
    try:
        df1 = pd.read_csv("/part-%05d"%i, header=None)
        df1[0] = df1[0].apply( lambda x: float(x.strip("(([]))\n").strip(")")))
        df1[1] = df1[1].apply( lambda x: float(x.strip("(([]))\n").strip(")")))
        df = pd.concat([df1,df])
    except:
        continue
la=list(df[0].values)
lo=list(df[1].values)
data = zip(la,lo)
c_la,c_lon=zip(*data)   

st1 = pd.read_csv("../station_data/part-00000", header=None)
st2 = pd.read_csv("../station_data/part-00001", header=None)
st = pd.concat([st1,st2])
st[0] = st[0].apply(lambda x: float(x.split("=")[0].strip("()")))
st[1] = st[1].apply(lambda x: float(x.split("=")[1].strip("()")))
st_la=list(st[2].values)
st_lo=list(st[3].values)
st_data = zip(st_la,st_lo)
st_la,st_lon=zip(*st_data)

gmap = gmplot.GoogleMapPlotter.from_geocode("New York City")
for i in range(len(la)):
    gmap.marker(la[i],lo[i] , 'cornflowerblue')
gmap.scatter(st_la, st_lon, '#8B0000', size=80, marker=False)
gmap.draw("clusters_station.html")

print("Done!")
