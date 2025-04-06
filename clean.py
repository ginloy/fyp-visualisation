import polars as pl
import numpy as np

cols = [
    "id1",
    "id2",
    "config",
    "accel_pos",
    "x",
    "data_real",
    "data_imag",
    "ordinate_axis_units_lab",
]
temp = pl.struct("data_real", "data_imag").map_elements(
    lambda x: complex(x["data_real"], x["data_imag"]), return_dtype=pl.Object
)
df = (
    pl.scan_parquet("./data.pq")
    .select(cols)
    .with_columns(
        np.sqrt(pl.col("data_real") ** 2 + pl.col("data_imag") ** 2)
        .cast(pl.Float64)
        .alias("data_mag"),
        np.arctan(pl.col("data_imag") / pl.col("data_real"))
        .cast(pl.Float64)
        .alias("data_phase"),
        pl.col("id2").str.extract(r"([+-]\w)\(\w\)$", group_index=1).alias("axis"),
        pl.col("id2").str.extract(r"^\w+\(([E_R]+)\)", group_index=1).alias("E/R"),
        pl.col("ordinate_axis_units_lab").alias("units"),
    )
    .drop("ordinate_axis_units_lab")
)

fftdf = df.filter(
    pl.col("id1") == "FFT", pl.col("config") == "E9", pl.col("accel_pos") == "4"
)
input_fft = fftdf.filter(pl.col("E/R") == "E").select(
    "x", pl.struct("data_real", "data_imag").alias("data")
)
output_fft = fftdf.filter(pl.col("E/R") == "R").select(
    "x", pl.struct("data_real", "data_imag").alias("data"), "axis"
)
axes = ["+X", "+Y", "+Z"]
x = input_fft.select("x").collect().to_series().to_numpy()
input_data = input_fft.select("data").collect().to_series().to_numpy()
input_data = np.vectorize(complex)(input_data[:, 0], input_data[:, 1])
new_dfs = []
for axis in axes:
    output_fft_axis = output_fft.filter(pl.col("axis") == axis)
    output_data = output_fft_axis.select("data").collect().to_series().to_numpy()
    output_data = np.vectorize(complex)(output_data[:, 0], output_data[:, 1])
    frf = output_data / input_data
    new_df = (
        pl.DataFrame(
            dict(
                x=x,
                data_real=np.real(frf),
                data_imag=np.imag(frf),
                data_mag=np.abs(frf),
                data_phase=np.angle(frf),
            )
        )
        .lazy()
        .with_columns(
            id1=pl.lit("FRF"),
            id2=pl.lit(f"H(E_R)1_-Z,2_+{axis}(f)"),
            config=pl.lit("E9"),
            accel_pos=pl.lit("4"),
            axis=pl.lit(axis),
            **{"E/R": pl.lit("E_R")},
            units=pl.lit("m/s^2"),
        )
    )
    new_dfs.append(new_df)

df = pl.concat([df, *new_dfs], how="diagonal").sort(
    ["config", "accel_pos", "axis", "id2", "x"]
)
df.sink_parquet("cleaned.pq")
