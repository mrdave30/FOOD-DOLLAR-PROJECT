FILES SCOPE

This collection of R scripts implements a comprehensive and reproducible workflow for the construction and harmonization of international Input–Output Tables (IOT) based on the EORA 26 database, with a specific focus on the food and agriculture sectors. 
Even if EORA 26 is the source database, trade and transportation margins are derived from the FULL EORA database.
The objective is to generate consistent, country-level datasets that allow the decomposition of the “food dollar” into primary production, processing, trade, and transport margins. In fact, while the EORA database is constituted of a multiregional DB, the Food Value Chain methodology needs National IOT.
The main script, FOOD_DOLLAR_PROJECT.R, performs the extraction, labeling, and transformation of raw EORA26 sub-matrices:

•	VA – Value Added
•	T (Z) – Intra industries transaction
•	FD – final demand

for each year and country to build the main national IOT (inclusive of imports and exports, that are computed in the script).

It then applies the RAS balancing algorithm to enforce accounting consistency and identities between total industrial inputs (Z column sum) and output (Z row sum).
Fore selected industries related to the FVC, and namely:
•	Agriculture
•	Fishing
•	Wholesale 
•	Retail Trade
•	Food Manufacturing

Data granularity is increased using Undata (and the industrial quota in the value added in particular).

The second and third scripts, TRADE_MARGINS.R and TRANSPORT_MARGINS.R, extend this process by isolating the trade and transportation margins embedded in inter-sectoral flows. Both scripts reorganize national IOTs into block-diagonal matrices, apply concordance tables to align EORA 26 and FULL EORA DB, and compute agriculture and food-related margins using country-specific share coefficients (from Agriculture_share.csv and food_beverages_share.csv). These procedures yield harmonized datasets of trade and transport margins by country, year, and sector. The application of trade and transportation margins to the agricultural sector is needed to assess the consumption (purchaser prices) of the agricultural commodities and prepare the data to the decomposition analysis in STATA.
