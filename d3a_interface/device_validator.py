"""
Copyright 2018 Grid Singularity
This file is part of D3A.

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
"""
import ast

from d3a_interface.constants_limits import ConstSettings
from d3a_interface.exceptions import D3ADeviceException
from d3a_interface.utils import key_in_dict_and_not_none, key_in_dict_and_not_none_and_not_str_type
from d3a_interface.validators.utils import validate_range_limit

GeneralSettings = ConstSettings.GeneralSettings
LoadSettings = ConstSettings.LoadSettings
PvSettings = ConstSettings.PVSettings
HomeMeterSettings = ConstSettings.HomeMeterSettings
StorageSettings = ConstSettings.StorageSettings
CepSettings = ConstSettings.CommercialProducerSettings


def validate_storage_device(**kwargs):
    if "initial_soc" in kwargs and kwargs["initial_soc"] is not None:
        error_message = \
            {"misconfiguration": [f"initial_soc should be in between "
                                  f"{StorageSettings.INITIAL_CHARGE_LIMIT.min} & "
                                  f"{StorageSettings.INITIAL_CHARGE_LIMIT.max}."]}
        validate_range_limit(StorageSettings.INITIAL_CHARGE_LIMIT.min,
                             kwargs["initial_soc"],
                             StorageSettings.INITIAL_CHARGE_LIMIT.max, error_message)

    if "min_allowed_soc" in kwargs and kwargs["min_allowed_soc"] is not None:
        error_message = \
            {"misconfiguration": [f"min_allowed_soc should be in between "
                                  f"{StorageSettings.MIN_SOC_LIMIT.min} & "
                                  f"{StorageSettings.MIN_SOC_LIMIT.max}."]}
        validate_range_limit(StorageSettings.MIN_SOC_LIMIT.min,
                             kwargs["min_allowed_soc"],
                             StorageSettings.MIN_SOC_LIMIT.max, error_message)

    if ("initial_soc" in kwargs and kwargs["initial_soc"] is not None) and \
            ("min_allowed_soc" in kwargs and kwargs["min_allowed_soc"] is not None) and \
            (kwargs["initial_soc"] < kwargs["min_allowed_soc"]):
        raise D3ADeviceException(
            {"misconfiguration": [f"initial_soc should be greater "
                                  f"than or equal to min_allowed_soc."]})

    if "battery_capacity_kWh" in kwargs and kwargs["battery_capacity_kWh"] is not None:
        error_message = \
            {"misconfiguration": [f"battery_capacity_kWh should be in between "
                                  f"{StorageSettings.CAPACITY_LIMIT.min} & "
                                  f"{StorageSettings.CAPACITY_LIMIT.max}."]}
        validate_range_limit(StorageSettings.CAPACITY_LIMIT.min,
                             kwargs["battery_capacity_kWh"],
                             StorageSettings.CAPACITY_LIMIT.max, error_message)
    if "max_abs_battery_power_kW" in kwargs and kwargs["max_abs_battery_power_kW"] is not None:
        error_message = \
            {"misconfiguration": [f"max_abs_battery_power_kW should be in between "
                                  f"{StorageSettings.MAX_ABS_POWER_RANGE.initial} & "
                                  f"{StorageSettings.MAX_ABS_POWER_RANGE.final}."]}
        validate_range_limit(StorageSettings.MAX_ABS_POWER_RANGE.initial,
                             kwargs["max_abs_battery_power_kW"],
                             StorageSettings.MAX_ABS_POWER_RANGE.final, error_message)

    if "initial_selling_rate" in kwargs and kwargs["initial_selling_rate"] is not None:
        error_message = \
            {"misconfiguration": [f"initial_selling_rate should be in between "
                                  f"{StorageSettings.INITIAL_SELLING_RATE_LIMIT.min} & "
                                  f"{StorageSettings.INITIAL_SELLING_RATE_LIMIT.max}."]}
        validate_range_limit(StorageSettings.INITIAL_SELLING_RATE_LIMIT.min,
                             kwargs["initial_selling_rate"],
                             StorageSettings.INITIAL_SELLING_RATE_LIMIT.max, error_message)

    if "final_selling_rate" in kwargs and kwargs["final_selling_rate"] is not None:
        error_message = \
            {"misconfiguration": [f"final_selling_rate should be in between "
                                  f"{StorageSettings.FINAL_SELLING_RATE_LIMIT.min} & "
                                  f"{StorageSettings.FINAL_SELLING_RATE_LIMIT.max}."]}
        validate_range_limit(StorageSettings.FINAL_SELLING_RATE_LIMIT.min,
                             kwargs["final_selling_rate"],
                             StorageSettings.FINAL_SELLING_RATE_LIMIT.max, error_message)

    if ("initial_selling_rate" in kwargs and kwargs["initial_selling_rate"] is not None) and \
            ("final_selling_rate" in kwargs and kwargs["final_selling_rate"] is not None) and \
            (kwargs["initial_selling_rate"] < kwargs["final_selling_rate"]):
        raise D3ADeviceException({"misconfiguration": [f"initial_selling_rate should be greater "
                                                       f"than or equal to final_selling_rate."]})

    if "initial_buying_rate" in kwargs and kwargs["initial_buying_rate"] is not None:
        error_message = \
            {"misconfiguration": [f"initial_buying_rate should be in between "
                                  f"{StorageSettings.INITIAL_BUYING_RATE_LIMIT.min} & "
                                  f"{StorageSettings.INITIAL_BUYING_RATE_LIMIT.max}."]}
        validate_range_limit(StorageSettings.INITIAL_BUYING_RATE_LIMIT.min,
                             kwargs["initial_buying_rate"],
                             StorageSettings.INITIAL_BUYING_RATE_LIMIT.max, error_message)

    if "final_buying_rate" in kwargs and kwargs["final_buying_rate"] is not None:
        error_message = {"misconfiguration": [f"final_buying_rate should be in between "
                                              f"{StorageSettings.FINAL_BUYING_RATE_LIMIT.min} & "
                                              f"{StorageSettings.FINAL_BUYING_RATE_LIMIT.max}."]}
        validate_range_limit(StorageSettings.FINAL_BUYING_RATE_LIMIT.min,
                             kwargs["final_buying_rate"],
                             StorageSettings.FINAL_BUYING_RATE_LIMIT.max, error_message)
    if ("initial_buying_rate" in kwargs and kwargs["initial_buying_rate"] is not None) and \
            ("final_buying_rate" in kwargs and kwargs["final_buying_rate"] is not None) and \
            (kwargs["initial_buying_rate"] > kwargs["final_buying_rate"]):
        raise D3ADeviceException(
            {"misconfiguration": [f"initial_buying_rate should be less "
                                  f"than or equal to final_buying_rate."]})
    if ("final_selling_rate" in kwargs and kwargs["final_selling_rate"] is not None) and \
            ("final_buying_rate" in kwargs and kwargs["final_buying_rate"] is not None) and \
            (kwargs["final_buying_rate"] > kwargs["final_selling_rate"]):
        raise D3ADeviceException(
            {"misconfiguration": [f"final_buying_rate should be less "
                                  f"than or equal to final_selling_rate."]})

    if "energy_rate_increase_per_update" in kwargs and \
            kwargs["energy_rate_increase_per_update"] is not None:
        error_message = \
            {"misconfiguration": [f"energy_rate_increase_per_update should be in between "
                                  f"{GeneralSettings.RATE_CHANGE_PER_UPDATE_LIMIT.min} & "
                                  f"{GeneralSettings.RATE_CHANGE_PER_UPDATE_LIMIT.max}."]}
        validate_range_limit(GeneralSettings.RATE_CHANGE_PER_UPDATE_LIMIT.min,
                             kwargs["energy_rate_increase_per_update"],
                             GeneralSettings.RATE_CHANGE_PER_UPDATE_LIMIT.max, error_message)

    if "energy_rate_decrease_per_update" in kwargs and \
            kwargs["energy_rate_decrease_per_update"] is not None:
        error_message = \
            {"misconfiguration": [f"energy_rate_decrease_per_update should be in between "
                                  f"{GeneralSettings.RATE_CHANGE_PER_UPDATE_LIMIT.min} & "
                                  f"{GeneralSettings.RATE_CHANGE_PER_UPDATE_LIMIT.max}."]}
        validate_range_limit(GeneralSettings.RATE_CHANGE_PER_UPDATE_LIMIT.min,
                             kwargs["energy_rate_decrease_per_update"],
                             GeneralSettings.RATE_CHANGE_PER_UPDATE_LIMIT.max, error_message)

    if kwargs.get("fit_to_limit") is True and (
            kwargs.get("energy_rate_increase_per_update") is not None
            or kwargs.get("energy_rate_decrease_per_update") is not None):
        raise D3ADeviceException(
            {"misconfiguration": [
                "fit_to_limit & energy_rate_increase/decrease_per_update can't be set together."]})
    if kwargs.get("fit_to_limit") is False and (
            kwargs.get("energy_rate_increase_per_update") is None
            or kwargs.get("energy_rate_decrease_per_update") is None):
        raise D3ADeviceException(
            {"misconfiguration": [
                "energy_rate_increase/decrease_per_update must be set if fit_to_limit is False."]})

    if key_in_dict_and_not_none(kwargs, "loss_function"):
        error_message = {"misconfiguration": [f"loss_function should either be "
                                              f"{StorageSettings.LOSS_FUNCTION_LIMIT.min} or "
                                              f"{StorageSettings.LOSS_FUNCTION_LIMIT.max}."]}
        validate_range_limit(StorageSettings.LOSS_FUNCTION_LIMIT.min, kwargs["loss_function"],
                             StorageSettings.LOSS_FUNCTION_LIMIT.max, error_message)
        if key_in_dict_and_not_none(kwargs, "loss_per_hour"):
            if kwargs["loss_function"] == 1:
                error_message = {
                    "misconfiguration": [f"loss_per_hour should be in between "
                                         f"{StorageSettings.LOSS_PER_HOUR_RELATIVE_LIMIT.min} & "
                                         f"{StorageSettings.LOSS_PER_HOUR_RELATIVE_LIMIT.max}."]}
                validate_range_limit(StorageSettings.LOSS_PER_HOUR_RELATIVE_LIMIT.min,
                                     kwargs["loss_per_hour"],
                                     StorageSettings.LOSS_PER_HOUR_RELATIVE_LIMIT.max,
                                     error_message)
            else:
                error_message = {
                    "misconfiguration": [f"loss_per_hour should be in between "
                                         f"{StorageSettings.LOSS_PER_HOUR_ABSOLUTE_LIMIT.min} & "
                                         f"{StorageSettings.LOSS_PER_HOUR_ABSOLUTE_LIMIT.max}."]}
                validate_range_limit(StorageSettings.LOSS_PER_HOUR_ABSOLUTE_LIMIT.min,
                                     kwargs["loss_per_hour"],
                                     StorageSettings.LOSS_PER_HOUR_ABSOLUTE_LIMIT.max,
                                     error_message)


def _validate_rate(energy_rate):
    error_message = \
        {"misconfiguration": [f"energy_rate should be in between "
                              f"{CepSettings.ENERGY_RATE_LIMIT.min} & "
                              f"{CepSettings.ENERGY_RATE_LIMIT.max}."]}
    validate_range_limit(CepSettings.ENERGY_RATE_LIMIT.min, energy_rate,
                         CepSettings.ENERGY_RATE_LIMIT.max, error_message)


def _validate_rate_profile(energy_rate_profile):
    for date, value in energy_rate_profile.items():
        value = float(value) if type(value) == str else value
        error_message = \
            {"misconfiguration": [f"energy_rate should at time: {date} be in between "
                                  f"{CepSettings.ENERGY_RATE_LIMIT.min} & "
                                  f"{CepSettings.ENERGY_RATE_LIMIT.max}."]}
        validate_range_limit(CepSettings.ENERGY_RATE_LIMIT.min, value,
                             CepSettings.ENERGY_RATE_LIMIT.max, error_message)


def validate_energy_rate(**kwargs):
    if "energy_rate" in kwargs and kwargs["energy_rate"] is not None:
        if isinstance(kwargs["energy_rate"], (float, int)):
            _validate_rate(kwargs["energy_rate"])
        elif isinstance(kwargs["energy_rate"], str):
            _validate_rate_profile(ast.literal_eval(kwargs["energy_rate"]))
        elif isinstance(kwargs["energy_rate"], dict):
            _validate_rate_profile(kwargs["energy_rate"])
        else:
            raise D3ADeviceException({"misconfiguration": [f"energy_rate has an invalid type."]})


def validate_commercial_producer(**kwargs):
    validate_energy_rate(**kwargs)


def validate_market_maker(**kwargs):
    validate_energy_rate(**kwargs)
    if "energy_rate_profile" in kwargs and kwargs["energy_rate_profile"] is not None and \
            ("energy_rate_profile_uuid" not in kwargs or
             kwargs["energy_rate_profile_uuid"] is None):
        raise D3ADeviceException(
            {"misconfiguration": [f"energy_rate_profile must have a uuid."]})
    if "grid_connected" in kwargs and kwargs["grid_connected"] is not None and \
            not isinstance(kwargs["grid_connected"], bool):
        raise D3ADeviceException(
            {"misconfiguration": [f"grid_connected must be a boolean value."]})


def validate_infinite_bus(**kwargs):
    validate_commercial_producer(**kwargs)
    if "energy_rate_profile" in kwargs and kwargs["energy_rate_profile"] is not None and \
            ("energy_rate_profile_uuid" not in kwargs or
             kwargs["energy_rate_profile_uuid"] is None):
        raise D3ADeviceException(
            {"misconfiguration": [f"energy_rate_profile must have a uuid."]})
    if key_in_dict_and_not_none_and_not_str_type(kwargs, "energy_rate_profile_uuid"):
        raise D3ADeviceException(
            {"misconfiguration": [f"energy_rate_profile_uuid must have a string type."]})

    if key_in_dict_and_not_none(kwargs, "energy_buy_rate"):
        _validate_rate(kwargs['energy_buy_rate'])
    if key_in_dict_and_not_none(kwargs, "buying_rate_profile") and \
            ("buying_rate_profile_uuid" not in kwargs or
             kwargs["buying_rate_profile_uuid"] is None):
        raise D3ADeviceException(
            {"misconfiguration": [f"buying_rate_profile must have a uuid."]})
    if key_in_dict_and_not_none_and_not_str_type(kwargs, "buying_rate_profile_uuid"):
        raise D3ADeviceException(
            {"misconfiguration": [f"buying_rate_profile_uuid must have a string type."]})


def validate_finite_diesel_generator(**kwargs):
    if "max_available_power_kW" in kwargs and kwargs["max_available_power_kW"] is not None:
        if isinstance(kwargs["max_available_power_kW"], (int, float)):
            error_message = \
                {"misconfiguration": [f"max_available_power_kW should be in between "
                                      f"{CepSettings.MAX_POWER_KW_LIMIT.min} & "
                                      f"{CepSettings.MAX_POWER_KW_LIMIT.max}."]}
            validate_range_limit(CepSettings.MAX_POWER_KW_LIMIT.min,
                                 kwargs["max_available_power_kW"],
                                 CepSettings.MAX_POWER_KW_LIMIT.max,
                                 error_message)
        elif isinstance(kwargs["max_available_power_kW"], dict):
            error_message = \
                {"misconfiguration": [f"max_available_power_kW should be in between "
                                      f"{CepSettings.MAX_POWER_KW_LIMIT.min} & "
                                      f"{CepSettings.MAX_POWER_KW_LIMIT.max}."]}
            for date, value in kwargs["max_available_power_kW"].items():
                validate_range_limit(CepSettings.MAX_POWER_KW_LIMIT.min, value,
                                     CepSettings.MAX_POWER_KW_LIMIT.max, error_message)
        else:
            raise D3ADeviceException({"misconfiguration": [f"max_available_power_kW has an "
                                                           f"invalid type. "]})

    validate_commercial_producer(**kwargs)
