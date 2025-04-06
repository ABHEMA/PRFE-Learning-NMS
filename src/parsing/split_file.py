import pandas as pd
import math

# Chemin du fichier CSV merged d'entrée
input_csv = "merged_dataset.csv"
# Préfixe pour les fichiers de sortie
output_prefix = "merged_dataset_part"

# Charger le fichier CSV dans un DataFrame
df = pd.read_csv(input_csv)

# Nombre total de lignes dans le fichier
total_rows = len(df)
print(f"Nombre total de lignes : {total_rows}")

# Calculer la taille de chaque segment (nombre de lignes par fichier)
chunk_size = math.ceil(total_rows / 4)
print(f"Taille de chaque segment : environ {chunk_size} lignes")

# Découper le DataFrame en 4 segments et sauvegarder chacun dans un fichier CSV séparé
for i in range(4):
    start_idx = i * chunk_size
    end_idx = min((i + 1) * chunk_size, total_rows)
    chunk = df.iloc[start_idx:end_idx]
    output_file = f"{output_prefix}{i+1}.csv"
    chunk.to_csv(output_file, index=False)
    print(f"Segment {i+1} sauvegardé : lignes {start_idx} à {end_idx-1} dans {output_file}")
