import geopandas as gpd
import pandas as pd
import os
from google.cloud import bigquery

def main():
    # Set up BigQuery client (should inherit permissions from the automation platform)
    client = bigquery.Client()
    
    # Read from the table created by SQL Runner
    # You'll need to adjust this table name to match what SQL Runner creates
    table_name = "your_dataset.all_regions_turfs"  # Adjust this
    
    query = f"SELECT * FROM `{table_name}`"
    all_regions_turfs = client.query(query).to_dataframe()
    
    # Load the master shapefile (assuming this is available in your environment)
    # You might need to adjust the path or read this from GCS/BigQuery
    andy_shapefile = gpd.read_file('data/master_precinct_shapes.csv')
    
    # Ensure GEOID columns are strings for proper merging
    all_regions_turfs['GEOID'] = all_regions_turfs['GEOID'].astype(str)
    andy_shapefile['GEOID'] = andy_shapefile['GEOID'].astype(str)
    
    # Keep only necessary columns from shapefile
    andy_shapefile = andy_shapefile[['CountyName', 'PrcnctName', 'GEOID', 'geometry', 'WKT']]
    
    # Merge turf data with geometry
    all_turfs_with_geo = all_regions_turfs.merge(
        andy_shapefile[['GEOID', 'geometry']], 
        on='GEOID', 
        how='left'
    )
    
    # Convert to GeoDataFrame
    all_turfs_gdf = gpd.GeoDataFrame(all_turfs_with_geo, geometry='geometry')
    all_turfs_gdf.crs = andy_shapefile.crs
    
    # Create WKT format
    all_turfs_gdf['WKT'] = all_turfs_gdf['geometry'].to_wkt()
    
    # Create output directory
    os.makedirs('output', exist_ok=True)
    
    # Process each region
    for region, region_data in all_turfs_gdf.groupby('Region'):
        print(f"Processing {region}...")
        
        # Save complete region file
        region_filename = f"output/{region.lower()}_turf_shapes.csv"
        region_data.to_csv(region_filename, index=False)
        print(f"Saved {region_filename} with {len(region_data)} rows")
        
        # Split by individual turfs within the region
        for turf_name, turf_data in region_data.groupby('fo_name'):
            # Clean the filename - remove problematic characters and strip trailing underscores
            filename = turf_name.replace(' ', '_').replace('-', '').lower().strip('_')
            turf_filename = f"output/{filename}.csv"
            turf_data.to_csv(turf_filename, index=False)
            print(f"  Saved {turf_filename} with {len(turf_data)} rows")
    
    # Save master file with all regions
    master_filename = "output/all_regions_turf_shapes.csv"
    all_turfs_gdf.to_csv(master_filename, index=False)
    print(f"Saved master file {master_filename} with {len(all_turfs_gdf)} rows")
    
    # Summary statistics
    print("\n=== SUMMARY ===")
    region_summary = all_turfs_gdf.groupby('Region').agg({
        'van_precinct_id': 'count',
        'voters': 'sum',
        'doors': 'sum',
        'targets': 'sum'
    }).rename(columns={'van_precinct_id': 'precincts'})
    
    print(region_summary)
    
    turf_summary = all_turfs_gdf.groupby(['Region', 'fo_name']).size().reset_index(name='precincts')
    print(f"\nTotal turfs across all regions: {len(turf_summary)}")
    print(f"Total precincts across all regions: {len(all_turfs_gdf)}")

if __name__ == "__main__":
    main()