// Copyright Vespa.ai. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.

package com.yahoo.prelude.fastsearch;

import com.yahoo.data.access.Inspector;

/**
 * Class representing a byte field in the result set
 *
 * @author bratseth
 */
public class BoolField extends DocsumField {

    public BoolField(String name) {
        super(name);
    }

    @Override
    public Object convert(Inspector value) { return value.asBool(); }

}
