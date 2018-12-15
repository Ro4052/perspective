/******************************************************************************
 *
 * Copyright (c) 2018, the Perspective Authors.
 *
 * This file is part of the Perspective library, distributed under the terms of
 * the Apache License 2.0.  The full license can be found in the LICENSE file.
 *
 */

import {DateParser, DATE_PARSE_CANDIDATES} from "./DateParser.js";
import {get_column_type} from "../utils.js";
import moment from "moment";

export class DataAccessor {
    constructor() {
        this.data_formats = {
            row: 1,
            column: 2,
            schema: 3
        };
        this.format = undefined;
        this.data = undefined;
        this.column_names = undefined;
        this.data_types = undefined;
        this.row_count = 0;
        // TODO: optimize and refactor out
        this.moment = moment;
        this.candidates = DATE_PARSE_CANDIDATES;
    }

    extract_typevec(typevec) {
        let types = [];
        for (let i = 0; i < typevec.size() - 1; i++) {
            types.push(typevec.get(i));
        }
        return types;
    }

    is_format(data) {
        if (Array.isArray(data)) {
            return this.data_formats.row;
        } else if (Array.isArray(data[Object.keys(data)[0]])) {
            return this.data_formats.column;
        } else if (typeof data[Object.keys(data)[0]] === "string" || typeof data[Object.keys(data)[0]] === "function") {
            return this.data_formats.schema;
        } else {
            throw "Unknown data format!";
        }
    }

    get_row_count(data) {
        if (this.format === this.data_formats.row) {
            return data.length;
        } else if (this.format === this.data_formats.column) {
            return data[Object.keys(data)[0]].length;
        } else {
            return 0;
        }
    }

    get(column_name, row_index) {
        let value = undefined;

        if (this.format === this.data_formats.row) {
            let d = this.data[row_index];
            if (d !== undefined && d.hasOwnProperty(column_name)) {
                value = d[column_name];
            }
        } else if (this.format === this.data_formats.column) {
            if (this.data.hasOwnProperty(column_name)) {
                value = this.data[column_name][row_index];
            }
        } else if (this.format === this.data_formats.schema) {
            value = undefined;
        } else {
            throw "Unknown data format!";
        }

        return value;
    }

    marshal(column_name, row_index, type) {
        let val = clean_data(this.get(column_name, row_index));

        if (val === null) {
            return null;
        }

        if (typeof val === "undefined") {
            return undefined;
        }

        const date_parser = new DateParser();
        switch (get_column_type(type.value)) {
            case "float": {
                val = Number(val);
                break;
            }
            case "integer": {
                val = Number(val);
                // FIXME: bring this back in
                if (val > 2147483647 || val < -2147483648) {
                    // This handles cases where a long sequence of e.g. 0 precedes a clearly
                    // float value in an inferred column.  Would not be needed if the type inference
                    // checked the entire column, or we could reset parsing.
                    //this.data_types[this.column_names.indexOf(name)] = __MODULE__.t_dtype.DTYPE_FLOAT64;
                }
                break;
            }
            case "boolean": {
                if (typeof val === "string") {
                    val.toLowerCase() === "true" ? (val = true) : (val = false);
                } else {
                    val = !!val;
                }
                break;
            }
            case "datetime":
            case "date": {
                val = date_parser.parse(val);
                break;
            }
            default: {
                val === null ? (val = null) : (val += ""); // TODO this is not right - might not be a string.  Need a data cleaner
            }
        }

        return val;
    }

    make_columnar_data(__MODULE__, data) {
        let cdata = [];
        let row_count = 0;

        if (this.format === this.data_formats.row) {
            if (data.length === 0) {
                throw "Not yet implemented: instantiate empty grid without column type";
            }

            for (let name of this.column_names) {
                let col = [];
                for (let i = 0; i < data.length; i++) {
                    col.push(this.marshal(name, i, this.data_types[Object.keys(this.column_names).indexOf(name)].value));
                }

                cdata.push(col);
                row_count = col.length;
            }
        } else if (this.format === this.data_formats.column) {
            row_count = data[Object.keys(data)[0]].length;
            for (let name of this.column_names) {
                // Extract the data or fill with undefined if column doesn't exist (nothing in column changed)
                let transformed;
                if (data.hasOwnProperty(name)) {
                    transformed = data[name].map(clean_data);
                } else {
                    transformed = new Array(row_count);
                }
                cdata.push(transformed);
            }
        } else if (this.format === this.data_formats.schema) {
            // eslint-disable-next-line no-unused-vars
            for (let name in data) {
                cdata.push([]);
            }
        }

        return [cdata, row_count];
    }

    /**
     * Links the accessor to a package of data for processing,
     * calculating its format and size.
     *
     * @private
     * @param {object} __MODULE__: the Module object generated by Emscripten
     * @param {object} data
     *
     * @returns An object with 5 properties:
     *    cdata - an array of columnar data.
     *    names - the column names.
     *    types - the column t_dtypes.
     *    row_count - the number of rows per column.
     *    is_arrow - an internal flag marking arrow-formatted data
     */
    init(__MODULE__, data) {
        this.data = data;
        this.format = this.is_format(data);
        this.row_count = this.get_row_count(data);
    }

    /**
     * Converts supported inputs into canonical data for
     * interfacing with perspective.
     *
     * @private
     * @param {object} __MODULE__: the Module object generated by Emscripten
     * @param {object} data
     *
     * @returns An object with 5 properties:
     *    cdata - an array of columnar data.
     *    names - the column names.
     *    types - the column t_dtypes.
     *    row_count - the number of rows per column.
     *    is_arrow - an internal flag marking arrow-formatted data
     */
    parse(__MODULE__, data) {
        this.data = data;
        this.format = this.is_format(data);
        this.column_names = __MODULE__.column_names(data, this.format);
        this.data_types = __MODULE__.data_types(data, this.format, this.column_names, moment, DATE_PARSE_CANDIDATES);

        let [cdata, row_count] = this.make_columnar_data(__MODULE__, data);
        this.row_count = row_count;

        return {
            cdata,
            names: this.column_names,
            types: this.data_types,
            row_count,
            is_arrow: false
        };
    }

    /**
     * Convert data with given names and types for
     * interfacing with perspective.
     *
     * @private
     * @param {object} __MODULE__: the Module object generated by Emscripten
     * @param {object} data
     * @param {Array} names
     * @param {Array} data_types
     *
     * @returns An object with 5 properties:
     *    cdata - an array of columnar data.
     *    names - the column names.
     *    types - the column t_dtypes.
     *    row_count - the number of rows per column.
     *    is_arrow - an internal flag marking arrow-formatted data
     */
    update(__MODULE__, data, names, data_types) {
        this.format = this.is_format(data);
        this.column_names = names;

        let types = this.extract_typevec(data_types);
        this.data_types = types;

        let [cdata, row_count] = this.make_columnar_data(__MODULE__, data);
        this.row_count = row_count;

        return {cdata, names, types, row_count, is_arrow: false};
    }
}

/**
 * Coerce string null into value null.
 * @private
 * @param {*} value
 */
export function clean_data(value) {
    if (value === null || value === "null") {
        return null;
    } else if (value === undefined || value === "undefined") {
        return undefined;
    } else {
        return value;
    }
}
