"""Common sanity checks to apply when a dataset has been updated.

"""

import abc
import base64
import os
from datetime import datetime

import numpy as np
import pandas as pd
import plotly
import plotly.express as px
from tqdm.auto import tqdm

# Label to use for old dataset.
DATA_LABEL_OLD = "old"
# Label to use for new dataset.
DATA_LABEL_NEW = "new"
# True to include interactive plots in output HTML file (which can make the inspection slow if there are many figures);
# False to include plots as static images.
EXPORT_INTERACTIVE_PLOTS = False
# Maximum number of plots (of potentially problematic cases) to show in output file.
MAX_NUM_PLOTS = 150


def rename_columns(
        data, entities_to_rename_in_columns, name_dataset, name_default):
    """Translate columns in a dataframe, from a naming convention to another.

    Parameters
    ----------
    data : pd.DataFrame
        Input dataset.
    entities_to_rename_in_columns : list
        Entities (using default naming) that need to be translated in dataset.
    name_dataset : dict
        Dictionary of entity names in the dataset to be translated.
    name_default : dict
        Default entity names.

    Returns
    -------
    data_renamed : pd.DataFrame
        Dataset with columns adjusted to default naming.

    """
    columns_renaming = {}
    for entity in entities_to_rename_in_columns:
        # Ensure column exists in dictionary of entity names for considered dataset.
        error = f"ERROR: Entity {entity} is not defined in dictionary of entity names for this dataset."
        assert entity in name_dataset, error

        # Ensure column exists in default dictionary of entity names.
        error = f"ERROR: Entity {entity} is not defined in default dictionary of entity names."
        assert entity in name_default, error

        columns_renaming[name_dataset[entity]] = name_default[entity]

    data_renamed = data.rename(columns=columns_renaming)

    return data_renamed


def mean_absolute_percentage_error(old, new, epsilon=1e-6):
    """Mean absolute percentage error (MAPE).

    Parameters
    ----------
    old : pd.Series
        Old values.
    new : pd.Series
        New values.
    epsilon : float
        Small number that avoids divisions by zero.

    Returns
    -------
    error : float
        MAPE.

    """
    error = np.mean(abs(new - old) / (old + epsilon)) * 100

    return error


class Check(abc.ABC):
    """Common abstract check."""

    def __init__(self):
        self.num_warnings = 0

    @staticmethod
    def _parse_check_name(check_name):
        check_name_parsed = check_name.capitalize().replace('_', ' ')

        return check_name_parsed

    def _gather_all_check_names(self):
        all_check_names = [check_name for check_name in dir(self) if callable(getattr(self, check_name))
                           if check_name.startswith("check_")]

        return all_check_names
    
    def apply_all_checks(self):
        """Apply all methods in the class that are called 'check_', assume they do not ingest any argument, and execute
        them one by one, gathering all their output summaries.

        Returns
        -------
        all_warnings : pd.DataFrame
            All potentially problematic data points found during checks.

        """
        all_check_names = self._gather_all_check_names()
        all_warnings = pd.DataFrame({'check_name': []})
        for i, check_name in enumerate(tqdm(all_check_names)):
            check_name_parsed = self._parse_check_name(check_name)
            print(f"({i + 1}/{len(all_check_names)}) {check_name_parsed}.")
            # Execute check.
            warnings = getattr(self, check_name)()
            if len(warnings) > 0:
                print(f"WARNING: Potential issues: {len(warnings)}")
                warnings['check_name'] = check_name
                all_warnings = pd.concat([all_warnings, warnings], ignore_index=True)

        return all_warnings
    
    def summarize_warnings_in_html(self, all_warnings):
        """Generate a HTML summary of all warnings generated during sanity checks, that should be visually inspected.

        Parameters
        ----------
        all_warnings : pd.DataFrame
            All potentially problematic data points found during checks.


        Returns
        -------
        summary : str
            HTML with a summary of the results of this check.

        """
        all_check_names = self._gather_all_check_names()
        summary = ""
        for i, check_name in enumerate(all_check_names):
            check_name_parsed = self._parse_check_name(check_name)
            summary += f"<br><br>({i + 1}/{len(all_check_names)}) {check_name_parsed}."
            warnings = all_warnings[all_warnings['check_name'] == check_name].dropna(axis=1)
            if len(warnings) > 0:
                summary += f"<br><font color='red'>WARNING: Potential issues: {len(warnings)}</font>"
                summary += warnings.to_html()

        return summary


class SanityChecksOnSingleDataset(Check):
    def __init__(
        self,
        data,
        name,
        variable_ranges,
        min_year_latest_possible=None,
        max_year_maximum_delay=None,
        population=None,
    ):
        """Sanity checks to apply to a single dataset.

        Parameters
        ----------
        data : pd.DataFrame
            Dataset.
        name : dict
            Default entity names.
        variable_ranges : dict
            Ranges of values of all variables. Each key in this dictionary corresponds to a variable (defined using
            default entity names), and the value is another dictionary, containing at least the following keys:
            * 'min': Minimum value allowed for variable.
            * 'max': Maximum value allowed for variable. As a special case, the value for 'max' can be 'World', in which
              case we assume that the maximum value allowed for variable is the maximum value for the world.
        min_year_latest_possible : int
            Latest acceptable value for the minimum year.
        max_year_maximum_delay : int
            Maximum delay (from current year) that maximum year can have
        population : pd.DataFrame
            Population dataset.

        """
        super().__init__()
        self.data = data
        self.name = name
        self.variable_ranges = variable_ranges
        self.population = population
        self.min_year_latest_possible = min_year_latest_possible
        self.max_year_maximum_delay = max_year_maximum_delay

    def check_that_countries_are_in_population_dataset(self):
        missing_countries = list(set(self.data[self.name["country"]]) - set(self.population[self.name["country"]]))
        warnings = pd.DataFrame({'country': missing_countries})

        return warnings
    
    def _get_year_ranges(self):
        # Keep only rows for which we have at least one not null data point.
        data_clean = self.data.dropna(how="all")
        year_ranges = (
            data_clean.groupby(self.name["country"])
            .agg({self.name["year"]: (min, max)})[self.name["year"]]
            .reset_index()
        )

        return year_ranges
    
    def check_that_first_year_is_not_too_recent(self):
        year_ranges = self._get_year_ranges()
        warnings = year_ranges[year_ranges["min"] > self.min_year_latest_possible][[self.name['country'], 'min']].\
            rename(columns={'min': 'Value'})

        return warnings

    def check_that_latest_year_is_not_too_old(self):
        current_year = datetime.today().year
        year_ranges = self._get_year_ranges()
        warnings = year_ranges[year_ranges["max"] > (current_year - self.max_year_maximum_delay)][[
            self.name['country'], 'max']].rename(columns={'max': 'Value'})

        return warnings

    def check_that_variables_have_values_that_are_not_lower_than_expected(self):
        warnings = pd.DataFrame()
        ranges = {variable: self.variable_ranges[variable] for variable in self.variable_ranges
                  if self.name[variable] in self.data.columns}
        for variable in ranges:
            min_value = ranges[variable]['min']
            if min_value == 'World':
                min_value = self.data[(self.data[self.name['country']] == 'World')][self.name[variable]].min()
            too_low_rows = self.data[(self.data[self.name[variable]] < min_value)][[
                self.name['country'], self.name['year'], self.name[variable]]]
            if len(too_low_rows) > 0:
                too_low = too_low_rows.groupby(self.name['country']).agg({self.name[variable]: min}).\
                    reset_index().rename(columns={self.name[variable]: 'Value'})
                too_low['Variable'] = variable
                warnings = pd.concat([warnings, too_low], ignore_index=True)

        return warnings

    def check_that_variables_have_values_that_are_not_higher_than_expected(self):
        warnings = pd.DataFrame()
        ranges = {variable: self.variable_ranges[variable] for variable in self.variable_ranges
                  if self.name[variable] in self.data.columns}
        for variable in ranges:
            max_value = ranges[variable]['max']
            if max_value == 'World':
                max_value = self.data[(self.data[self.name['country']] == 'World')][self.name[variable]].max()
            too_high_rows = self.data[(self.data[self.name[variable]] > max_value)][[
                self.name['country'], self.name['year'], self.name[variable]]]
            if len(too_high_rows) > 0:
                too_high = too_high_rows.groupby(self.name['country']).agg({self.name[variable]: max}).\
                    reset_index().rename(columns={self.name[variable]: 'Value'})
                too_high['Variable'] = variable
                warnings = pd.concat([warnings, too_high], ignore_index=True)

        return warnings


class SanityChecksComparingTwoDatasets(Check):
    def __init__(
        self,
        data_old,
        data_new,
        error_metric,
        name=None,
        variable_ranges=None,
        data_label_old=DATA_LABEL_OLD,
        data_label_new=DATA_LABEL_NEW,
        export_interactive_plots=EXPORT_INTERACTIVE_PLOTS,
        max_num_plots=MAX_NUM_PLOTS,
    ):
        """Sanity checks comparing a new dataset with an old one.

        Parameters
        ----------
        data_old : pd.DataFrame
            Old dataset.
        data_new : pd.DataFrame
            New dataset.
        error_metric : dict
            Error metric. This dictionary should contain:
            * 'name': Name of the error metric.
            * 'function': Function that ingests and old array and a new array, and returns an array of errors.
            * 'min_relevant': Minimum error to consider for warnings. Any error below this value will be ignored.
        name : dict
            Default entity names.
        variable_ranges : dict
            Ranges of values of all variables. Each key in this dictionary corresponds to a variable (defined using
            default entity names), and the value is another dictionary, containing at least the following keys:
            * 'min_relevant': Minimum relevant value to consider when looking for abrupt deviations. If a variable has
              an *absolute value* smaller than min_relevant, both in old and new datasets, the deviation will not be
              calculated for that point. This is so to avoid inspecting large errors on small, irrelevant values.
        data_label_old : str
            Label to use to identify old dataset (in plots).
        data_label_new : str
            Label to use to identify new dataset (in plots).
        export_interactive_plots : bool
            True to export figures as interactive plotly figures (which, if there are many, can slow down the inspection
            of figures). False to export figures encoded in base64 format.
        max_num_plots : int
            Maximum number of plots to include in final summary of warnings. If there are more warnings than this
            number, an additional warning line is added before the figures.

        """
        super().__init__()
        self.data_old = data_old
        self.data_new = data_new
        self.data_label_old = data_label_old
        self.data_label_new = data_label_new
        self.name = name
        self.variable_ranges = variable_ranges
        self.error_metric = error_metric
        self.export_interactive_plots = export_interactive_plots
        self.max_num_plots = max_num_plots
        # Create comparison dataframe.
        self.comparison = self._create_comparison_dataframe()

    def _create_comparison_dataframe(self):
        data_old_prepared = self.data_old.copy()
        data_old_prepared["source"] = self.data_label_old
        data_new_prepared = self.data_new.copy()
        data_new_prepared["source"] = self.data_label_new
        comparison = pd.concat(
            [data_old_prepared, data_new_prepared], ignore_index=True
        )

        return comparison

    def check_that_all_countries_in_old_dataset_are_in_new_dataset(self):
        warnings = self.data_old[~self.data_old[self.name['country']].isin(self.data_new[self.name['country']])][[
            self.name['country']]].drop_duplicates()

        return warnings

    def check_that_all_countries_in_new_dataset_are_in_old_dataset(self):
        warnings = self.data_new[~self.data_new[self.name['country']].isin(self.data_old[self.name['country']])][[
            self.name['country']]].drop_duplicates()

        return warnings

    def check_that_all_columns_in_old_dataset_are_in_new_dataset(self):
        missing_columns = list(set(self.data_old.columns) - set(self.data_new.columns))
        warnings = pd.DataFrame({'Variable': missing_columns})

        return warnings

    def check_that_all_columns_in_new_dataset_are_in_old_dataset(self):
        missing_columns = list(set(self.data_new.columns) - set(self.data_old.columns))
        warnings = pd.DataFrame({'Variable': missing_columns})

        return warnings
    
    def plot_time_series_for_country_and_variable(self, country, variable):
        """Plot a time series for a specific country and variable in the old dataset and an analogous time series for
        the new dataset.

        Parameters
        ----------
        country : str
            Country.
        variable : str
            Entity name for the variable to plot.

        Returns
        -------
        fig : plotly.Figure
            Plot.

        """
        # Select data for country.
        comparison = self.comparison[
            self.comparison[self.name["country"]] == country
        ].reset_index(drop=True)[[self.name["year"], variable, "source"]]
        # Add columns for plotting parameters.
        comparison["size"] = 0.003
        comparison.loc[comparison["source"] == self.data_label_new, "size"] = 0.001
        # hover_data = {'source': False, name['year']: False, variable: True, 'size': False}
        hover_data = {}
        
        # Get range for vertical axis.
        absolute_min = comparison[variable].min()
        absolute_max = comparison[variable].max()

        fig = (
            px.scatter(
                comparison,
                x=self.name["year"],
                y=variable,
                color="source",
                size="size",
                size_max=10,
                color_discrete_sequence=["red", "green"],
                opacity=0.9,
                hover_name=self.name["year"],
                hover_data=hover_data,
            )
            .update_xaxes(
                showgrid=True,
                title="Year",
                autorange=False,
                range=[
                    comparison[self.name["year"]].min() - 1,
                    comparison[self.name["year"]].max() + 1,
                ],
            )
            .update_yaxes(
                showgrid=True,
                title=variable,
                autorange=False,
                range=[
                    absolute_min - abs(absolute_min) * 0.1,
                    absolute_max + abs(absolute_max) * 0.1,
                ],
            )
            .update_layout(
                clickmode="event+select", autosize=True, title=f"{country} - {variable}"
            )
            .update_layout(font={"size": 9})
        )

        return fig

    def get_value_deviations_from_old_to_new_datasets(self, error_name, error_function):
        """Compute the deviation between old and new datasets (by means of an error metric) for all countries and a list
        of specific variables.

        We expect the new dataset to not deviate much from the old one. Therefore, we compute the deviation between
        both, to detect potential issues with any of the datasets.

        Parameters
        ----------
        error_name : str
            Name for the error metric.
        error_function : function
            Error function, that must ingest an 'old' and 'new' arguments.

        Returns
        -------
        errors : pd.DataFrame
            Table of countries and variables, and their respective deviations (in terms of the given error metric).

        """
        # Compare only columns that appear in both old and new dataset, and have int or float dtypes.
        columns = [col for col in self.variable_ranges
                   if self.name[col] in self.data_old.columns
                   if self.name[col] in self.data_new.columns]
        errors = pd.DataFrame()
        for country in tqdm(self.comparison[self.name["country"]].unique().tolist()):
            for variable in columns:
                min_relevant_value = self.variable_ranges[variable]['min_relevant']
                comparison_pivot = (
                    self.comparison[self.comparison[self.name["country"]] == country]
                    .pivot(index=self.name["year"], columns="source", values=self.name[variable])
                    .dropna(how="any")
                    .reset_index()
                )
                for source in [self.data_label_old, self.data_label_new]:
                    if source not in comparison_pivot:
                        comparison_pivot[source] = np.nan
                # Omit rows where both old and new values are too small (to avoid large errors on irrelevant values).
                comparison_pivot = comparison_pivot[
                    (abs(comparison_pivot[self.data_label_old]) > min_relevant_value)
                    | (abs(comparison_pivot[self.data_label_new]) > min_relevant_value)
                ].reset_index(drop=True)
                error = error_function(
                    old=comparison_pivot[self.data_label_old],
                    new=comparison_pivot[self.data_label_new],
                )
                errors = pd.concat(
                    [
                        errors,
                        pd.DataFrame(
                            {
                                self.name["country"]: [country],
                                "Variable": [self.name[variable]],
                                error_name: [error],
                            }
                        ),
                    ],
                    ignore_index=True,
                )

        # Compare errors with mean value of a variables.
        mean_variable_value = (
            self.comparison.groupby([self.name["country"]])
            .mean()
            .reset_index()
            .drop(columns=self.name["year"])
            .melt(
                id_vars=self.name["country"],
                var_name="Variable",
                value_name="mean_variable_value",
            )
        )
        errors = pd.merge(
            mean_variable_value,
            errors,
            on=[self.name["country"], "Variable"],
            how="inner",
        )

        return errors

    def plot_all_metrics_for_country(self, country, variables):
        """Plot all metrics for a given country.

        Parameters
        ----------
        country : str
            Country to consider.
        variables : list
            Variables (given by their default entity names) to be compared.

        """
        for variable in variables:
            fig = self.plot_time_series_for_country_and_variable(
                country=country, variable=variable
            )
            fig.show()

    @staticmethod
    def convert_plotly_figure_to_html(fig, export_interactive_plots=EXPORT_INTERACTIVE_PLOTS):
        if export_interactive_plots:
            fig_html = fig.to_html(full_html=False, include_plotlyjs="cdn")
        else:
            img = plotly.io.to_image(fig, scale=1.2)
            img_base64 = base64.b64encode(img).decode("utf8")
            fig_html = f"<br><img class='icon' src='data:image/png;base64,{img_base64}'>"

        return fig_html

    def save_comparison_plots_for_specific_variables_of_country(
        self, country, variables, output_file
    ):
        """Save plots comparing the old and new time series of a list of variables, for a specific country, in an HTML
        file.

        For example, to save all comparisons of a list of variables for a specific country:
        save_comparison_plots_for_specific_variables_of_country(
            comparison=comparison, country='United Kingdom',
            variables=['Electricity from solar (TWh)', 'Electricity from wind (TWh)'])

        Parameters
        ----------
        country : str
            Country to consider.
        variables : list
            Variables (given by their default entity names) to be compared.
        output_file : str
            Path to output HTML file.

        """
        # Ensure output folder exists.
        output_dir = os.path.dirname(output_file)
        if not os.path.isdir(output_dir):
            os.makedirs(output_dir)
        # Create an HTML file with all figures.
        with open(output_file, "w") as f:
            for variable in variables:
                fig = self.plot_time_series_for_country_and_variable(
                    country=country, variable=variable
                )
                fig_html = self.convert_plotly_figure_to_html(
                    fig, export_interactive_plots=self.export_interactive_plots)
                f.write(fig_html)

    def check_that_values_did_not_change_abruptly_from_old_to_new_dataset(self):
        # Calculate errors between old and new time series.
        error_name = self.error_metric['name']
        error_function = self.error_metric['function']
        error_min_relevant = self.error_metric['min_relevant']
        errors = self.get_value_deviations_from_old_to_new_datasets(
            error_name=error_name, error_function=error_function)
        errors[error_name] = errors[error_name].round(1)
        warnings = errors[(errors[error_name] > error_min_relevant)].sort_values(error_name, ascending=False)

        return warnings

    def summarize_figures_to_inspect_in_html(self, warnings):
        """Plot time series from the old and new dataset, in certain, potentially problematic cases (country-variables),
        and return all these plots together as a string (HTML or base64).

        Parameters
        ----------
        warnings : pd.DataFrame
            Potentially problematic data points.

        Returns
        -------
        figures : str
            Figures, placed one after the other, in HTML or base64 format.

        """
        figures = ""
        warnings_to_plot = warnings[warnings[self.error_metric['name']].notnull()].reset_index(drop=True)
        if len(warnings_to_plot) > self.max_num_plots:
            figures += (
                f"<br><font color='red'>WARNING: {len(warnings_to_plot)} figures to plot, only {self.max_num_plots} "
                f"will be shown.</font><br>"
            )
        for i, warning in tqdm(warnings_to_plot.iterrows(), total=len(warnings_to_plot)):
            fig = self.plot_time_series_for_country_and_variable(
                country=warning[self.name["country"]], variable=warning["Variable"]
            )
            if self.error_metric['name'] in warning:
                fig.update_layout(
                    title=f"{fig.layout.title['text']} - Relevant {self.error_metric['name']}: "
                          f"{warning[self.error_metric['name']]} % "
                )
            fig_html = self.convert_plotly_figure_to_html(fig, export_interactive_plots=self.export_interactive_plots)
            figures += fig_html

        return figures
