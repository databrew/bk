# Locus GIS

"Locus GIS" is an offline mapping / GPS tool. It is available for download on Google Play [here](https://play.google.com/store/apps/details?id=menion.android.locus.gis&hl=en&gl=US).

## Setting up

For the purposes of the Bohemia Kenya project, Locus GIS must be configured by a data manager, in-person. Configuration consists of (a) installing the application, (b) creating a new "project", (c) adding new layers to that project.

### Installing the application

- On the device to [Goolge Play](https://play.google.com/store/apps/details?id=menion.android.locus.gis&hl=en&gl=US) and download/install the Locus GIS application
- Once installed, open the application

### Creating a new project

- In the upper left of the main screen, click the three horizontal bars
- Click on "Projects"
- In the bottom right, click the "plus" icon
- Click "New project"
- Name the new project "Bohemia"

### Set the right Coordinate Reference System (CRS) for the project
- Click the 3 horizontal bars in the upper left
- Click Projects
- Click Bohemia
- Click the 3 vertical dots
- Click Edit project
- Ensure that "Coordinate reference system" is WGS 84 / EPSG: 84 (see below screenshot)
![image](https://user-images.githubusercontent.com/4364537/224106288-ae05cb2f-e636-4365-9918-48aac2148c3e.png)



### Adding new layers to that project

- On your computer, go to [this google drive link](https://drive.google.com/drive/folders/1vth1P46HAPOJJc2lt0nKtV9VcLqK-cg6?usp=sharing)
- Download the contents of the folder to your computer
- Go to drive.google.com, logged in to your personal account
- Upload the folder to your personal drive
- On the device, open the Locus GIS application
- Click the "layers" icon in the upper right (three overlapping squares)
- Click the "plus" icon in the bottom right
- Click "Import shp file"
- Click "Google Drive"
- Select `KENYA CLUSTERS/clusters/clusters.shp` from the menu
- Do the same for `KENYA CLUSTERS/households/households.shp`


## Use

To use the application:

- Open the application
- In the upper left of the main screen, click the three horizontal bars
- Click "Projects"
- Click "Bohemia"
- Scroll around the map to see the locations of layers (clusters and households)
- Click an object (cluster or household) for information on that layer
- To navigate to a specific household based on map location:
	- Click on the household dot
	- Selec the household ID
	- Review the attribute table (ie, information about that household) to ensure it's correct
	- In the bottom right, click the icon with a small arrow and circle
- To find a household based on ID:
	- In the upper left of the main screen, click the three horizontal bars
	- Click "Layers"
	- Next to "households" click the 3 vertical dots to the right
	- Click "Attribute table"
	- Select the househol ID
	- In the bottom right, click the icon with a small arrow and circle
