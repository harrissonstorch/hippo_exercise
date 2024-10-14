import json
import os
import argparse
from collections import defaultdict, Counter
from utils import read_json_files_from_directory, read_csv_files_from_directory

def calculate_metrics(claims, reverts):
    metrics = defaultdict(lambda: defaultdict(lambda: {
        'fills': 0, 'reverted': 0, 'total_price': 0.0, 'total_quantity': 0
    }))
    
    claims_by_id = {}
    for claim in claims:
        #print(claim['id'])
        npi = claim['npi']
        ndc = claim['ndc']
        claims_by_id[claim['id']] = claim
        metrics[npi][ndc]['fills'] += 1
        metrics[npi][ndc]['total_price'] += claim['price']
        metrics[npi][ndc]['total_quantity'] += claim['quantity']
        
    for revert in reverts:
        claim_id = revert['claim_id']
        
        if claim_id in claims_by_id:
            npi = claims_by_id[claim_id]['npi']
            ndc = claims_by_id[claim_id]['ndc']
            metrics[npi][ndc]['reverted'] += 1
    
    # Calculate averages
    metrics_list = []
    for npi, ndc_data in metrics.items():
        for ndc, data in ndc_data.items():
            avg_price = data['total_price'] / data['total_quantity']
            metrics_list.append({
                'npi': npi,
                'ndc': ndc,
                'fills': data['fills'],
                'reverted': data['reverted'],
                'avg_price': round(avg_price,2),
                'total_price': round(data['total_price'],2)
            })

    return metrics_list

def generate_recommendations(claims, pharmacies):
    chain_prices = defaultdict(lambda: defaultdict(list))
    
    for claim in claims:
        ndc = claim['ndc']
        npi = claim['npi']
        pharmacy_chain = next((pharmacy['chain'] for pharmacy in pharmacies if pharmacy['npi'] == npi), None)
        
        if pharmacy_chain:
            unit_price = claim['price'] / claim['quantity']
            chain_prices[ndc][pharmacy_chain].append(unit_price)

    recommendations = []
    for ndc, chains in chain_prices.items():
        avg_prices = [
            {"name": chain, "avg_price": round((sum(prices) / len(prices)),2)}
            for chain, prices in chains.items()
        ]
        avg_prices.sort(key=lambda x: x['avg_price'])
        recommendations.append({
            'ndc': ndc,
            'chain': avg_prices[:2]
        })
    
    return recommendations

def most_common_quantity(claims):
    quantity_counts = defaultdict(Counter)
    
    for claim in claims:
        ndc = claim['ndc']
        quantity = claim['quantity']
        quantity_counts[ndc][quantity] += 1
    
    most_prescribed = []
    for ndc, counter in quantity_counts.items():
        most_common = [item for item, _ in counter.most_common(5)]
        most_prescribed.append({
            'ndc': ndc,
            'most_prescribed_quantity': most_common
        })
    
    return most_prescribed

def main(pharmacy_dirs, claims_dirs, reverts_dirs):

    # Read pharmacy, claims and reverts data
    pharmacies = []
    for directory in pharmacy_dirs:
        pharmacies.extend(read_csv_files_from_directory(directory))

    claims = []
    for directory in claims_dirs:
        claims.extend(read_json_files_from_directory(directory))

    reverts = []
    for directory in reverts_dirs:
        reverts.extend(read_json_files_from_directory(directory))
    
    # Extract valid NPIs from pharmacies
    valid_npis = {pharmacy['npi'] for pharmacy in pharmacies}
    
    # Filter claims based on valid NPIs
    filtered_claims = [
        claim for claim in claims
        if claim.get('npi') in valid_npis and
           claim.get('quantity') is not None and claim.get('quantity') > 0 and
           claim.get('price') is not None and
           claim.get('ndc') is not None
    ]
    
    # Filter reverts based on whether its claim's NPI is valid
    filtered_reverts = [revert for revert in reverts if revert['claim_id'] in {claim['id'] for claim in filtered_claims}]

    # Calculate metrics
    metrics = calculate_metrics(filtered_claims, filtered_reverts)
    
    # Generate recommendations for top chains per drug
    recommendations = generate_recommendations(filtered_claims, pharmacies)
    
    # Determine most common quantity prescribed per drug
    common_quantities = most_common_quantity(filtered_claims)
    
    # Write output to JSON files in the results directory
    os.makedirs('results', exist_ok=True)
    
    with open('results/metrics.json', 'w') as f:
        json.dump(metrics, f, indent=4)
    
    with open('results/recommendations.json', 'w') as f:
        json.dump(recommendations, f, indent=4)
    
    with open('results/quantity_prescriptions.json', 'w') as f:
        json.dump(common_quantities, f, indent=4)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process pharmacy claims data.')
    parser.add_argument('--pharmacy_dirs', nargs='+', type=str, default=['data/pharmacies'],
                        help='List of directories containing pharmacy CSV files.')
    parser.add_argument('--claims_dirs', nargs='+', type=str, default=['data/claims'],
                        help='List of directories containing claims JSON files.')
    parser.add_argument('--reverts_dirs', nargs='+', type=str, default=['data/reverts'],
                        help='List of directories containing reverts JSON files.')

    args = parser.parse_args()

    main(pharmacy_dirs=args.pharmacy_dirs, claims_dirs=args.claims_dirs, reverts_dirs=args.reverts_dirs)

