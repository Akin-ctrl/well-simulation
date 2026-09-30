# Synthetic reference well: phase, integrity, and machine signals

## Purpose and boundary

The seeded fleet has 18 parameter types. The original one-second process model
calculated 11. This design supplies the remaining seven from the same process
state and adds oil, water, and gas rates so the phase arithmetic is visible.
The reference well is synthetic. Its geometry, fluid assay, rock strength,
steel condition, and machine properties are assumptions. A real well needs
measured inputs and an independent validation campaign before these outputs
can support operations.

`ProcessState` is calculated once per well per model tick. `derive_readings` is a
pure function of the current liquid flow, water cut, reservoir proxy pressure,
tubing pressure, temperature, and pump state. The gateway still converts one
complete fleet tick to the existing Modbus map. All 18 mapped readings retain
`synthetic_reduced_order_model` and `fieldCalibrated: false`.

## Units and equations

`q_l` is `flow_rate_bpd` in stock-tank barrels per day, `w` is
`water_cut_percent / 100`, `q_o` is `oil_rate_bpd`, and `q_w` is
`water_rate_bpd`. The phase balance is `q_o = q_l(1-w)` and `q_w = q_l w`.
The model assumes the reported liquid volume is additive at stock-tank
conditions. It has no shrinkage or water formation-volume factor.

For gas, `P_t` is `tubing_pressure_psi` and `P_b` is
`bubble_point_pressure_psi`, both converted from gauge to absolute pressure
using 14.6959 psi. `T` is `wellhead_temperature_f` and `T_ref` is
`reference_temperature_f`, both converted to kelvin. `R_sb` is
`solution_gas_at_bubble_point_scf_stb`, and `n` is
`solution_gas_pressure_exponent`. The empirical black-oil relation is
`R_s = min(R_sb, R_sb min(P_t/P_b, 1)^n T_ref/T)`. `R_free` is
`free_gas_oil_ratio_scf_stb`. Produced `gas_oil_ratio_scf_stb` is
`R_free + R_sb - R_s`; `gas_rate_scfd = q_o × gas_oil_ratio_scf_stb`.
The ratio and gas rate are zero when oil rate is zero. This is a declared
synthetic flash approximation, not a separator PVT calculation or conserved
reservoir gas inventory. Its input gas quantity and exponent need PVT tests.

`gas_h2s_mole_fraction` and `gas_co2_mole_fraction` are assumed dry produced-gas
mole fractions. The gas readings are `h2s_level_ppm = 10^6 × y_H2S` and
`co2_level_percent = 100 × y_CO2` while gas flows, otherwise zero. Here ppm
means gas mole parts per million, not aqueous or workplace concentration.
The model has no gas component transport or phase partition calculation.

`annulus_pressure_psi` describes a sealed outer casing annulus. It is distinct
from the existing `casing_pressure_psi` proxy for the inner tubing-casing
space. Its starting pressure is `annulus_initial_pressure_psi`. Its temperature
`T_a` starts at `ambient_temperature` and approaches
`ambient_temperature + annulus_thermal_coupling × (T - ambient_temperature)`
with `annulus_thermal_response_rate` per second. With fixed gas mass and volume,
`P_a,new(abs) = P_a,old(abs) × T_a,new(K) / T_a,old(K)`. There is no leak,
pressure control, or allowable-annulus-pressure calculation. A sustained
pressure rise in a real well requires well-integrity diagnosis, not this model.

For sand, `D = max(reservoir_pressure_proxy_psi - tubing_pressure_psi, 0)`
is a drawdown proxy in psi. `D_on` is `sand_onset_drawdown_psi` and `k_s` is
`sand_entrainment_kg_per_bbl_psi`. The assumed sand mass flux in kg/day is
`m_s = k_s × max(D-D_on, 0) × q_l² / max_flow_rate`. Liquid mass flux is
`m_l = liquid_density_kg_per_bbl × q_l`. `sand_detector_ppm` is
`10^6 × m_s/m_l` when `q_l > 0`, otherwise zero. Here ppm means kg of sand per
million kg of produced liquid. This onset surrogate omits rock stress,
perforation geometry, particle size, and detector response.

For corrosion, `y_CO2` times absolute tubing pressure gives CO2 partial
pressure `p_CO2` in psi. `f_w = w/(w + water_wetting_half_fraction)` is an
assumed wall-wetting factor. The assumed uniform corrosion current density is
`i = i_ref × f_w × (p_CO2/p_ref)^a × exp[E/R(1/T_ref - 1/T)] ×
(1 + corrosion_flow_factor × q_l/max_flow_rate)` in A/m². The inputs are
`reference_corrosion_current_a_m2`, `reference_co2_partial_pressure_psi`,
`co2_corrosion_exponent` (`a`), and `corrosion_activation_energy_j_mol` (`E`).
`R = 8.314462618 J/(mol K)`. Faraday's law converts it to uniform steel wall
loss: `v = i M_Fe/(2 F rho_steel)` in m/s, with `M_Fe = 0.055845 kg/mol`,
`F = 96485.33212 C/mol`, and `rho_steel = 7850 kg/m³`. The output
`corrosion_rate_mpy` is `v × 365.25 × 86400 × 39370.0787402` mils/year.
`corrosionFactor` remains a separate dimensionless damage index. This current
law is an assumed calibration surface, not NORSOK M-506, a coupon reading,
localized corrosion, or a sour-service materials assessment.

For vibration, the reference is one sensor direction on a pump-side structure.
`m` is `vibration_effective_mass_kg`, `f_n` is `vibration_natural_hz`,
`zeta` is `vibration_damping_ratio`, and `f` is
`vibration_excitation_hz`. With `omega = 2πf`, `omega_n = 2πf_n`,
`k = m omega_n²` N/m, and `c = 2 zeta m omega_n` N s/m, force amplitude is
`F_0 = pump_excitation_force_n × pump_flow_factor` while the pump is on, plus
`flow_excitation_force_n_per_bpd × q_l`. The steady harmonic velocity is
`v_rms = F_0 omega / [sqrt(2) sqrt((k-m omega²)² + (c omega)²)]`, returned
as `vibration_mm_s` in mm/s. This is a single-frequency RMS estimate. It is
not a broadband waveform, bearing diagnosis, or ISO acceptance grade.

## State update and failure rules

The original reduced-order liquid pressure and temperature equations remain.
The one-second pressure step uses the flow at the start of the interval. The
published flow is recalculated at the updated pressure, so all reported phase
and diagnostic values use the current state. The independent sealed-annulus
temperature and pressure then advance one second. No random input enters a
tick. Defaults are version `0.3.0`; every new parameter has a unit, range, and
restart requirement in `twin_core/model_defaults.json`.

Model parameters and controls reject invalid numbers. A state with a nonfinite
or negative diagnostic value is rejected. `derive_readings` rejects a nonfinite
or negative result. The gateway rejects a missing, nonfinite, negative, stale,
or mixed-tick field before changing any holding register. Ingestion writes a
batch only after the Modbus heartbeat is stable. Values are encoded as float32,
so historian precision is limited by the wire format. The fleet snapshot carries
model version 0.3.0, but the present Modbus and historian row format carries
only the source kind and tick time. Model-version lineage per stored reading
remains a persistence gap for Milestone 7.

## Verification and standards boundary

Tests check phase balance, gas-rate multiplication, gas liberation under lower
pressure, sand onset, the absolute-temperature annulus law, Faraday unit
conversion, pump and flow vibration response, zero-flow behavior, finite bounds,
and gateway rejection of incomplete fleet ticks. The running stack must show
12 wells × 18 readings per complete batch with the model source label.

[API RP 90-1](https://www.api.org/products-and-services/standards/important-standards-announcements/90-1)
covers offshore annular casing pressure management and diagnosis. This model
only respects the distinction between annulus spaces and absolute pressure.
[NORSOK M-506](https://standard.no/en/news/norsok-m-5062017-co2-corrosion-rate-calculation-model-is-on-systematic-review/)
is a CO2 corrosion calculation practice; the current law above does not
implement it. [ISO 20816](https://www.iso.org/standard/63180.html) addresses
machine vibration measurement and evaluation; this model reports a stated RMS
velocity quantity but has no machine-specific acceptance limit. No compliance
claim is made for these standards.

Field validation requires well geometry, pressure and temperature surveys,
fluid PVT and gas composition, separator tests, rock and sand data, coupons or
wall-loss history, annulus tests, and vibration sensor location and spectra.
Fit parameters on one measured period, hold out another period, compare each
output with uncertainty and bias, and reject the model for a use case whose
error exceeds a defined tolerance. Until then the `fieldCalibrated` flag stays
false and the readings are for simulation and software integration only.
