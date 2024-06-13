import pandas as pd

operations = ["get", "put", "delete", "singledelete", "rangedelete", "merge", "iterator_seek", "iterator_seekForPrev", "multiget"]

def load_data(csv_file):
    return pd.read_csv(csv_file, index_col=False)

def count_total_queries(data):
    access_count_columns = data.filter(like='_access_count').columns
    total_queries = data[access_count_columns].sum(axis=1).sum()
    return total_queries

def count_percentages(data):
    access_count_columns = data.filter(like='_access_count').columns
    column_sums = data[access_count_columns].sum()
    total_access_count = column_sums.sum()
    column_percentages = (column_sums / total_access_count) * 100
    rename_dict = {col: col.replace('_access_count', '').capitalize() for col in access_count_columns}
    column_percentages.rename(index=rename_dict, inplace=True)
    non_zero_percentages = column_percentages[column_percentages > 0]
    return non_zero_percentages

def convert_output(query_type):
    if query_type == 'Get':
        return 'Read'
    elif query_type == 'Put':
        return 'Write'
    elif query_type == 'Merge':
        return 'Merge/read-modify-write'
    else:
        return query_type

def profile_query_composition(data):
    if len(data) == 1:
        dominant_query = next(iter(data.items()))
        workload_type = f"{convert_output(dominant_query)} only"
    elif all(45 <= data.get(i, 0) <= 55 for i in [0, 1]):
        workload_type = "heavy updating"
    else:
        max_type = data.idxmax()
        workload_type = f"{convert_output(max_type)} heavy"
    return workload_type

def analyze_detailed_access_distribution(data):
    descriptions = []

    for op in operations:
        mean_key = f'{op}_mean'
        mode_key = f'{op}_mode'
        median_key = f'{op}_median'
        quartile1_key = f'{op}_quartiles[0]'
        quartile3_key = f'{op}_quartiles[2]'
        kurtosis_key = f'{op}_kurtosis'

        if mean_key in data.columns:
            mean = data[mean_key].iloc[0]
            mode = data[mode_key].iloc[0]
            median = data[median_key].iloc[0]
            quartile1 = data[quartile1_key].iloc[0] if quartile1_key in data.columns else None
            quartile3 = data[quartile3_key].iloc[0] if quartile3_key in data.columns else None
            kurtosis = data[kurtosis_key].iloc[0] if kurtosis_key in data.columns else None
            
            if mode == 0:
                continue
            
            description = f"For {op} requests:\n"
            description += f"  - The mean number of accesses per key is approximately {mean:.3f}.\n"
            description += f"  - The mode number of accesses per key is {mode}.\n"
            description += f"  - The median number of accesses per key is {median}.\n"
            if quartile1 is not None and quartile3 is not None:
                description += f"  - The first quartile (25%) is {quartile1}, and the third quartile (75%) is {quartile3}.\n"
            if kurtosis is not None:
                description += f"  - The kurtosis of the distribution is {kurtosis:.3f}. "

            if mode == 1:
                description += "This suggests that most keys are accessed once, indicating a workload where keys are typically accessed uniquely per session or period.\n"
            elif mean < 2:
                description += "This suggests a fairly even distribution with most keys accessed only a few times.\n"
            else:
                description += "This suggests that some keys may be accessed multiple times, indicating potential hotspots.\n"

            descriptions.append(description)
    
    return descriptions

def profile_size(data):
    messages = []

    for operation in operations:
        value_size_average_col = f"{operation}_value_size_average"
        value_size_median_col = f"{operation}_value_size_median"
        value_size_variance_col = f"{operation}_value_size_variance"
        
        if value_size_average_col in data.columns and value_size_median_col in data.columns and value_size_variance_col in data.columns:
            average = data[value_size_average_col].mean(skipna=True)
            median = data[value_size_median_col].mean(skipna=True)
            variance = data[value_size_variance_col].mean(skipna=True)

            if average > 0 and median > 0:
                if variance == 0:
                    message = (f"For {operation} operations:\n"
                            f"  Average value size is {average:.2f} bytes, with the median also at {median:.2f} bytes and zero variance, "
                            f"  showing consistent small-size data retrieval.")
                else:
                    message = (f"For {operation} operations:\n"
                            f"  Average value size is approximately {average:.2f} bytes with a larger variance (around {variance:.2f}), "
                            f"  suggesting more variability in the size of data being stored.")
                messages.append(message)

    return messages

def generate_summary(data):
    cf_num = 1
    non_zero_percentages = count_percentages(data)
    key_access_message = "".join(f"{message}\n" for message in analyze_detailed_access_distribution(data))
    key_value_sizes_message = "".join(f"{message}\n" for message in profile_size(data)) 

    query_composition = "The workload consists of " + ", ".join(f"{value:.2f}% {index}" for index, value in non_zero_percentages.items())
    query_composition += f", meaning its a {profile_query_composition(non_zero_percentages)} workload. There are {cf_num} column family in this workload"

    summary = f"""
The workload information are as follows:

1. Query Compositions
{query_composition} operations.

2. Key and Value Size Characteristics 
{key_value_sizes_message}

3. Key access distribution
{key_access_message}
"""
    return summary

def main(csv_file):
    data = load_data(csv_file)
    summary = generate_summary(data)
    print(summary)

# Example usage:
csv_file = '/home/nando/CLionProjects/rocksdb_temp/complete_traces/mixgraph/ml_feature.csv'
main(csv_file)
