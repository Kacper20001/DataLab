import matplotlib.pyplot as plt
import seaborn as sns
import os

from decorators.counter import count_calls
from decorators.timer import measure_time


@count_calls
@measure_time
def visualize_data(df):
    """
    Tworzy czytelne wykresy analizy danych taxi NYC.
    """
    os.makedirs("data/output", exist_ok=True)

    # ─────────────────────────────────────────────
    # Histogram długości trasy (do 30 mil)
    filtered_df = df[df["trip_distance"] <= 30]

    plt.figure(figsize=(10, 6))
    filtered_df["trip_distance"].hist(bins=50, edgecolor="black")
    plt.title("Rozkład długości trasy (mile) – tylko do 30 mil")
    plt.xlabel("Długość trasy (mile)")
    plt.ylabel("Liczba kursów")
    plt.tight_layout()
    plt.savefig("data/output/trip_distance_hist_filtered.png")
    plt.close()

    # ─────────────────────────────────────────────
    # Średni napiwek względem liczby pasażerów (tylko 1-6)
    df_tip = df[df["passenger_count"].between(1, 6)]
    tip_by_passengers = df_tip.groupby("passenger_count")["tip_amount"].mean()

    plt.figure(figsize=(10, 6))
    tip_by_passengers.plot(kind="bar", color="skyblue", edgecolor="black")
    plt.title("Średni napiwek ($) względem liczby pasażerów (1–6)")
    plt.xlabel("Liczba pasażerów")
    plt.ylabel("Średni napiwek ($)")
    plt.tight_layout()
    plt.savefig("data/output/tip_by_passenger_count_filtered.png")
    plt.close()

    # ─────────────────────────────────────────────
    # Boxplot napiwków (do 15$)
    df_tip30 = df[df["tip_amount"] <= 15]

    plt.figure(figsize=(10, 6))
    sns.boxplot(data=df_tip30[["tip_amount"]], orient="h", color="white", linewidth=1.5, fliersize=1, boxprops=dict(facecolor='lightblue'))
    plt.title("Rozrzut wartości napiwków (do 15$)")
    plt.xlabel("Wartość napiwku ($)")
    plt.tight_layout()
    plt.savefig("data/output/tip_amount_boxplot.png")
    plt.close()

    # ─────────────────────────────────────────────
    #  Scatter: całkowita kwota vs napiwek (do 100$)
    df_scatter = df[(df["total_amount"] <= 100) & (df["tip_amount"] <= 30)]

    plt.figure(figsize=(10, 6))
    plt.scatter(df_scatter["total_amount"], df_scatter["tip_amount"], alpha=0.05, s=5, color="green")
    plt.title("Korelacja: Całkowita kwota vs Napiwek")
    plt.xlabel("Całkowita kwota ($)")
    plt.ylabel("Napiwek ($)")
    plt.tight_layout()
    plt.savefig("data/output/tip_vs_total_scatter.png")
    plt.close()

    # ─────────────────────────────────────────────
    # Heatmapa średniej długości trasy vs liczba pasażerów (1–6)
    df_heat = df[df["passenger_count"].between(1, 6)]
    pivot = df_heat.pivot_table(index="passenger_count", values="trip_distance", aggfunc="mean")

    plt.figure(figsize=(8, 5))
    sns.heatmap(pivot, annot=True, cmap="YlGnBu", fmt=".1f")
    plt.title("Średnia długość trasy vs liczba pasażerów")
    plt.ylabel("Liczba pasażerów")
    plt.xlabel("trip_distance")
    plt.tight_layout()
    plt.savefig("data/output/passenger_vs_distance_heatmap.png")
    plt.close()

    print("[Visualizer] Wykresy zapisane do folderu: data/output/")
