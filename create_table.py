from database import DatabaseHandler


db_handler = DatabaseHandler()
 # Connect to the database
db_handler.connect()

data_last_update_sql = """
CREATE TABLE data_last_update (
    data_name varchar(255),
    last_update date ,
    PRIMARY KEY (data_name)
) """

tax_rate_sql = """
CREATE TABLE tax_rate (
    industry varchar(255),
    no_of_firms varchar(255),
    total_taxable_income varchar(255),
    total_taxes_paid_accrual varchar(255),
    total_cash_taxes_paid varchar(255),
    cash_taxes_accrual_taxes varchar(255),
    effectiveTR_avg_across_all_comp varchar(255),
    effectiveTR_avg_across_money_making_comp varchar(255),
    effectiveTR_agg_tax_rate varchar(255),
    cashTR_avg_across_money_making_comp varchar(255),
    cashTR_agg_tax_rate varchar(255),
    PRIMARY KEY (industry)
) """

sales_to_cap_us_sql = """
CREATE TABLE sales_to_cap_us (
    industry varchar(255),
    no_of_firms varchar(255),
    capex varchar(255),
    depre_amort varchar(255),
    capex_depre varchar(255),
    acquisitions varchar(255),
    net_r_and_d varchar(255),
    net_capex_sales varchar(255),
    net_capex_ebit_afterTax varchar(255),
    sales_invested_capital varchar(255),
    PRIMARY KEY (industry)
) """

beta_us_sql = """
CREATE TABLE beta_us (
    industry varchar(255),
    no_of_firms varchar(255),
    avg_unlevered_beta varchar(255),
    avg_levered_beta varchar(255),
    avg_correlation_with_mkt varchar(255),
    total_unlevered_beta varchar(255),
    total_levered_beta varchar(255),
    PRIMARY KEY (industry)
) """

pe_ratio_us_sql = """CREATE TABLE pe_ratio_us (
    industry varchar(255),
    no_of_firms varchar(255),
    perc_money_losing_firms_trailing varchar(255),
    current_pe varchar(255),
    trailing_pe varchar(255),
    forward_pe varchar(255),
    agg_mkt_cap_net_income varchar(255),
    agg_mkt_cap_trailing_net_income_money_making_firms varchar(255),
    expected_growth_next_5_yrs varchar(255),
    peg_ratio varchar(255),
    PRIMARY KEY (industry)
    )"""

rev_growth_rate_sql = """CREATE TABLE rev_growth_rate (
    industry varchar(255),
    no_of_firms varchar(255),
    cagr_net_income_last_5_years varchar(255),
    cagr_net_rev_last_5_years varchar(255),
    expected_growth_rev_next_2_years varchar(255),
    expected_growth_rev_next_5_years varchar(255),
    expected_growth_eps_next_5_years varchar(255),
    PRIMARY KEY (industry)
    )"""

ebit_growth_sql = """CREATE TABLE ebit_growth (
    industry varchar(255),
    no_of_firms varchar(255),
    roc varchar(255),
    reinvestment_rate varchar(255),
    expected_growth_ebit varchar(255),
    PRIMARY KEY (industry)
    )"""


db_handler.execute_query(ebit_growth_sql)