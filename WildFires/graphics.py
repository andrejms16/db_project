import os
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import geopandas as gpd
from utils import print_menu, text_standardize
from tabulate import tabulate


def graphics(db_instance):
    # Create the folder if it does not exist
    output_dir = 'graphics'
    os.makedirs(output_dir, exist_ok=True)
    options = [
        {'index': 0, 'text': 'Histogram and box-plot for every canadian fire index'},
        {'index': 1, 'text': 'Box-plot of the number of wildfires grouped by municipality, per year and district'},
        {'index': 2, 'text': 'Bar-plots of the number of wildfires by municipality for each district'},
        {'index': 3, 'text': 'Choropleth map of the number of wildfires by district.'},
        {'index': 4, 'text': 'Choropleth map of the average burned area per municipality in each district.'}
    ]
    selected = print_menu(options, 'graphics menu', 'Please select one of the previous options')
    if selected == 0:
        query = """
        SELECT
	        CANADIAN_FIRE_INDEX.ACRONYM AS ACRONYM,
	        FIRE_RISK_INDEX.INDEX_VALUE AS INDEX_VALUE
        FROM
            FIRE
            INNER JOIN FIRE_RISK_INDEX ON FIRE.ID = FIRE_RISK_INDEX.FIRE_ID
            JOIN CANADIAN_FIRE_INDEX ON FIRE_RISK_INDEX.CANADIAN_FIRE_INDEX_ID = CANADIAN_FIRE_INDEX.ID
        WHERE
            FIRE_RISK_INDEX.INDEX_VALUE IS NOT NULL
        ORDER BY
            FIRE_RISK_INDEX.INDEX_VALUE DESC
        """
        df = db_instance.custom_query(query, False)
        for index in df['acronym'].unique():
            filtered_df = df[df['acronym'] == index]
            hist_box(index, filtered_df['index_value'], 20, output_dir)
        print("Press Enter to continue...")
        input()
    if selected == 1:
        query = """
        SELECT
            FIRE.YEAR_NUMBER AS YEAR,
            DISTRICT.NAME AS DISTRICT,
            MUNICIPALITY.NAME AS MUNICIPALITY,
            COUNT(*) AS NUMBER_OF_WILDFIRES
        FROM
            FIRE
            JOIN NEIGHBORHOOD ON FIRE.NEIGHBORHOOD_ID = NEIGHBORHOOD.ID
            JOIN MUNICIPALITY ON NEIGHBORHOOD.MUNICIPALITY_ID = MUNICIPALITY.ID
            JOIN DISTRICT ON MUNICIPALITY.DISTRICT_ID = DISTRICT.ID
        GROUP BY
            FIRE.YEAR_NUMBER,
            DISTRICT.NAME,
            MUNICIPALITY.NAME
        """
        df = db_instance.custom_query(query, False)
        box_plot(df, 'year', 'number_of_wildfires', 'district', output_dir)
    if selected == 2:
        query = """
                SELECT
                    FIRE.YEAR_NUMBER AS YEAR,
                    DISTRICT.NAME AS DISTRICT,
                    MUNICIPALITY.NAME AS MUNICIPALITY,
                    COUNT(*) AS NUMBER_OF_WILDFIRES
                FROM
                    FIRE
                    JOIN NEIGHBORHOOD ON FIRE.NEIGHBORHOOD_ID = NEIGHBORHOOD.ID
                    JOIN MUNICIPALITY ON NEIGHBORHOOD.MUNICIPALITY_ID = MUNICIPALITY.ID
                    JOIN DISTRICT ON MUNICIPALITY.DISTRICT_ID = DISTRICT.ID
                GROUP BY
                    FIRE.YEAR_NUMBER,
                    DISTRICT.NAME,
                    MUNICIPALITY.NAME
                """
        df = db_instance.custom_query(query, False)
        for district in df['district'].unique():
            filtered_df = df[df['district'] == district]
            bar_plot(filtered_df, 'municipality', 'number_of_wildfires', district, None, output_dir)
    if selected == 3:
        query = """
        SELECT
            DISTRICT.NAME AS DISTRICT,	
            COUNT(*) AS NUMBER_OF_WILDFIRES
        FROM
            FIRE
            JOIN NEIGHBORHOOD ON FIRE.NEIGHBORHOOD_ID = NEIGHBORHOOD.ID
            JOIN MUNICIPALITY ON NEIGHBORHOOD.MUNICIPALITY_ID = MUNICIPALITY.ID
            JOIN DISTRICT ON MUNICIPALITY.DISTRICT_ID = DISTRICT.ID
        GROUP BY
	        DISTRICT.NAME
        """
        df = db_instance.custom_query(query, False)
        map_plot(df, output_dir, 'number_of_wildfires', 'Number of Wildfires per district')
    if selected == 4:
        query = """
        WITH
	BURNED_AREA_MUNICIPALITY AS (
		SELECT
			DISTRICT.NAME AS DISTRICT,
			MUNICIPALITY.NAME AS MUNICIPALITY,
			AVG(BURNED_AREA.BURNED_AREA) AS AVG_BURNED_AREA_MUNICIPALITY
		FROM
			FIRE
			JOIN NEIGHBORHOOD ON FIRE.NEIGHBORHOOD_ID = NEIGHBORHOOD.ID
			JOIN MUNICIPALITY ON NEIGHBORHOOD.MUNICIPALITY_ID = MUNICIPALITY.ID
			JOIN DISTRICT ON MUNICIPALITY.DISTRICT_ID = DISTRICT.ID
			JOIN BURNED_AREA ON FIRE.ID = BURNED_AREA.FIRE_ID
			JOIN AREA_TYPE ON BURNED_AREA.AREA_TYPE_ID = AREA_TYPE.ID
		WHERE
			AREA_TYPE.DESCRIPTION = 'TOT'
		GROUP BY
			DISTRICT.NAME,
			MUNICIPALITY.NAME
	    )
        SELECT
            DISTRICT,
            ROUND(AVG(AVG_BURNED_AREA_MUNICIPALITY), 2) AS AVG_BURNED_AREA
        FROM
            BURNED_AREA_MUNICIPALITY
        GROUP BY
            DISTRICT
        """
        df = db_instance.custom_query(query, False)
        map_plot(df, output_dir, 'avg_burned_area', 'Average burned area per municipality in each district')
    return None


def hist_box(var_name, var, bins, output_dir):
    try:
        print('Variable', var_name)

        # Create the figure with two subplots: histogram and boxplot
        fig, ax = plt.subplots(2, 1, sharex=True, figsize=(10, 5))

        # Draw histogram
        sns.histplot(x=var, ax=ax[0], bins=bins)
        ax[0].set_title(f'Histogram of {var_name}')  # Add title to the histogram

        # Draw boxplot with the mean indicated
        sns.boxplot(x=var, ax=ax[1], showmeans=True)
        ax[1].set_title(f'Boxplot of {var_name}')  # Add title to the boxplot

        # Add mean and median lines to the histogram
        ax[0].axvline(var.mean(), linestyle='--', color='green', label='Mean')
        ax[0].axvline(var.median(), linestyle='--', color='red', label='Median')

        # Add legend
        ax[0].legend()

        # Save the plots as images
        image_path = os.path.join(output_dir, f'{var_name}_hist_box.png')
        plt.savefig(image_path)

        # Display the plots in the console
        plt.show()
    except Exception as e:
        print(e)

    return None


def box_plot(df, x, y, hue, output_dir):
    try:

        plt.figure(figsize=(15, 5))  # Set the figure size for the plot
        sns.boxplot(df, x=x, y=y, hue=hue, palette='Set2')
        plt.xticks(rotation=90)  # Rotate x-axis labels for better readability
        # Add legend
        plt.legend(loc='upper center', ncol=3)
        # Save the plots as images
        image_path = os.path.join(output_dir, f'{x}_{y}_{hue}_hist_box.png')
        plt.savefig(image_path)
        # Display the plots in the console
        plt.show()
    except Exception as e:
        print(e)


def bar_plot(df, x, y, var_title, hue, output_dir):
    fig, ax = plt.subplots(figsize=(8, 6))  # Set the figure size for the plot
    if hue is not None:
        sns.barplot(data=df, x=x, y=y, hue=hue,
                    palette='Set2')  # Create the bar plot
    else:
        sns.barplot(data=df, x=x, y=y)
    plt.xticks(rotation=90)  # Rotate x-axis labels for better readability
    ax.set_title(f'{var_title}')
    # Save the plots as images
    image_path = os.path.join(output_dir, f'{var_title}_{x}_{y}_{hue}_hist_box.png')
    plt.savefig(image_path)
    plt.show()  # Display the plot


def map_plot(df, output_dir, column, title):
    portugal_districts = gpd.read_file("pt.shp", encoding="utf-8")

    islands = ['Azores', 'Madeira']
    island_indexes = portugal_districts[portugal_districts['name'].isin(islands)].index
    portugal_districts = portugal_districts.drop(island_indexes)
    portugal_districts['name'] = portugal_districts['name'].apply(text_standardize)

    portugal_districts = portugal_districts.merge(df, left_on='name', right_on='district', how='left')
    print(tabulate(portugal_districts, headers='keys', tablefmt='psql'))

    fig, ax = plt.subplots(figsize=(10, 12))
    sns.set_style("whitegrid")
    portugal_districts.plot(column=column, ax=ax, legend=True, edgecolor="black",
                            cmap='OrRd',  # Colormap (Orange to Red)
                            missing_kwds={'color': 'lightgrey', 'label': 'Missing Values'})

    for x, y, label in zip(portugal_districts.geometry.centroid.x, portugal_districts.geometry.centroid.y,
                           portugal_districts["name"]):
        ax.text(x, y, label, fontsize=8, ha="center")

    ax.set_title(title, fontsize=15)
    ax.set_axis_off()

    image_path = os.path.join(output_dir, f'{title}.png')
    plt.savefig(image_path)
    plt.show()
