package com.first.pkg.mapper;

import com.google.api.services.bigquery.model.TableRow;

public interface Mappings {
    TableRow apply(String input);
}
