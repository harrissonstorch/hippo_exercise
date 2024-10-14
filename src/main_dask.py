import json
import os
import argparse
from utils import read_csv_files_with_dask, read_json_files_with_dask
from dask_sql import Context
import dask.dataframe as dd
import pandas as pd
from dask.distributed import Client


c = Context()


def generate_recommendations(claims_df, reverts_df, pharmacy_df):
    
    c.create_table("claims", claims_df)
    c.create_table("reverts", reverts_df)
    c.create_table("pharmacy", pharmacy_df)


    query = """
        with avg_by_chain as (
            select c.ndc, 
                p.chain,
                round(AVG(c.price / c.quantity),2) avg_unit_price
            from pharmacy p
            inner join claims c on p.npi = c.npi
            where not exists (select 1 from reverts r where c.id = r.claim_id)
            group by 1,2),
        chain_rank as (SELECT ndc, 
            chain, 
            avg_unit_price,
            row_number() over (partition by ndc order by avg_unit_price) rank_number
        FROM avg_by_chain)
        select ndc,
            chain,
            avg_unit_price
        from chain_rank
        where rank_number <=2
        ORDER BY ndc, rank_number
        """
    
    # Execute SQL on Dask DataFrame
    result_ddf = c.sql(query)

    # Compute the result to obtain a Pandas DataFrame
    result_df = result_ddf.compute()

    final_output = []

    for ndc, group in result_df.groupby('ndc'):
        chains = group[['chain', 'avg_unit_price']].rename(columns={'chain': 'name', 'avg_unit_price': 'avg_price'}).to_dict(orient='records')
        final_output.append({
            'ndc': ndc,
            'chain': chains
    })

    return final_output

def most_common_quantity(claims_df):
    
    c.create_table("claims", claims_df)


    query = """
        with quantity_counts as (
                select ndc, 
                    quantity,
                    count(*) times_prescribed
                from claims
                group by 1,2),
            quantity_rank as (
                SELECT ndc, 
                    quantity,
                    row_number() over (partition by ndc order by times_prescribed desc) rank_number
                FROM quantity_counts)
            select ndc,
                quantity
            from quantity_rank
            where rank_number <=5
            ORDER BY ndc, rank_number 
        """
    
    # Execute SQL on Dask DataFrame
    result_ddf = c.sql(query)

    # Compute the result to obtain a Pandas DataFrame
    result_df = result_ddf.compute()

    final_output = []

    for ndc, group in result_df.groupby('ndc'):
        quantities = group['quantity'].tolist()
        final_output.append({
            'ndc': ndc,
            'most_prescribed_quantity': quantities
        })

    return final_output

def calculate_metrics(claims_df, reverts_df):
    # Group claims by 'npi' and 'ndc' and aggregate necessary metrics.
    grouped_claims = claims_df.groupby(['npi', 'ndc']).agg({
        'id': 'count',  # Fills
        'price': 'sum',  # Total price
        'quantity': 'sum'  # Total quantity
    }).rename(columns={'id': 'fills','price': 'total_price'}).reset_index()

    # Calculate average price
    grouped_claims['avg_price'] = grouped_claims['total_price'] / grouped_claims['quantity']
    
    # Join with reverts to count reverted claims
    # Counting reverts: group by 'claim_id' to capture all reverted
    reverts_count_df = reverts_df.merge(claims_df[['id', 'npi', 'ndc']], left_on='claim_id', right_on='id', how='inner')
    reverted_counts = reverts_count_df.groupby(['npi', 'ndc']).size().reset_index()
    reverted_counts.columns = ['npi', 'ndc', 'reverted']

    # Repartition for the merge below
    grouped_claims = grouped_claims.repartition(npartitions=10)
    reverted_counts = reverted_counts.repartition(npartitions=10)

    # Combining claims and reverts
    metrics_df = grouped_claims.merge(reverted_counts, on=['npi', 'ndc'], how='left').fillna(0)


    # Compute metrics into a Pandas DataFrame
    final_metrics = metrics_df.compute()
    final_metrics['reverted'] = final_metrics['reverted'].astype(int)
    final_metrics['total_price'] = final_metrics['total_price'].round(2)

    return final_metrics[['npi','ndc','fills','reverted','avg_price','total_price']]

def main_dask(pharmacy_dirs, claims_dirs, reverts_dirs):

    # Parallelizing the work given the 10 core instance mentioned in the exercise
    client = Client(n_workers=10, threads_per_worker=2, memory_limit='2GB')

    # Load data from the pharmacy CSV and claims/reverts JSON files
    pharmacies_df_list = [read_csv_files_with_dask([directory]) for directory in pharmacy_dirs]
    pharmacies_df = dd.concat(pharmacies_df_list)
    claims_df_list = [read_json_files_with_dask([directory]) for directory in claims_dirs]
    claims_df = dd.concat(claims_df_list)
    reverts_df_list = [read_json_files_with_dask([directory]) for directory in reverts_dirs]
    reverts_df = dd.concat(reverts_df_list)

    # Extract valid NPIs from pharmacies
    valid_npis = set(pharmacies_df['npi'].compute().tolist())

    # Filter claims based on valid NPIs and remove broken records after basic validation
    filtered_claims_df = claims_df[
        claims_df['npi'].isin(valid_npis) &
        claims_df['quantity'].notnull() & 
        claims_df['quantity'] > 0 &
        claims_df['price'].notnull() &
        claims_df['ndc'].notnull()
    ]

    # materialize to perform the join-like operation
    valid_claim_ids = filtered_claims_df['id'].compute()  
    
    # Filter reverts based on whether its claim's NPI is valid
    filtered_reverts_df = reverts_df[reverts_df['claim_id'].isin(valid_claim_ids)]

    # Calculate metrics
    metrics = calculate_metrics(filtered_claims_df, filtered_reverts_df)
    
    # Generate recommendations for top chains per drug
    recommendations = generate_recommendations(filtered_claims_df, filtered_reverts_df, pharmacies_df)
    
    # Determine most common quantity prescribed per drug
    common_quantities = most_common_quantity(filtered_claims_df)
    
    # Write output to JSON files in the results directory
    os.makedirs('results', exist_ok=True)
    
    metrics.to_json('results/metrics_dask.json', orient='records', lines=False, indent=4)
    
    with open('results/recommendations_dask.json', 'w') as f:
        json.dump(recommendations, f, indent=4)
    
    with open('results/quantity_prescriptions_dask.json', 'w') as f:
        json.dump(common_quantities, f, indent=4)
    
    client.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process pharmacy claims data.')
    parser.add_argument('--pharmacy_dirs', nargs='+', type=str, default=['data/pharmacies'],
                        help='List of directories containing pharmacy CSV files.')
    parser.add_argument('--claims_dirs', nargs='+', type=str, default=['data/claims'],
                        help='List of directories containing claims JSON files.')
    parser.add_argument('--reverts_dirs', nargs='+', type=str, default=['data/reverts'],
                        help='List of directories containing reverts JSON files.')

    args = parser.parse_args()

    main_dask(pharmacy_dirs=args.pharmacy_dirs, claims_dirs=args.claims_dirs, reverts_dirs=args.reverts_dirs)

