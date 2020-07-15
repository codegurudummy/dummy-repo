/*
 * Copyright 2018-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 */

package com.amazon.malygos.webapp.g2s2.onboarding;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import com.amazon.g2s2.constant.DataRegion;
import com.amazon.malygos.webapp.g2s2.RecordReader;

import amazon.platform.g2s2.entity.impl.KeyPatternResult;
import amazon.platform.g2s2.entity.impl.KeyResult;
import amazon.platform.g2s2.entity.impl.TableResult;

/**
 * Provides an abstraction over the validation of key patterns definitions.
 */
final class KeyPatternValidator {

    @Nonnull private final TableDefinitions tableDefinitions;
    @Nonnull private final RecordReader recordReader;

    /**
     * Constructor.
     *
     * @param tableDefinitions the {@link TableDefinitions} used as a reference during validation.
     * @param recordReader     the {@link RecordReader} used as a reference during
     *                         validation.
     */
    KeyPatternValidator(@Nonnull final TableDefinitions tableDefinitions,
                        @Nonnull final RecordReader recordReader) {
        this.tableDefinitions = tableDefinitions;
        this.recordReader = recordReader;
    }

    /**
     * Validates that the specified {@link KeyPatternResult} can be imported to G2S2. Returns all
     * {@link ValidationError ValidationErrors} associated with the key.
     *
     * @param keyPatternResult the new key pattern whose definition will be validated.
     *
     * @return all {@link ValidationError ValidationErrors} associated with this key pattern.
     */
    @Nonnull
    Collection<ValidationError> validate(@Nonnull final KeyPatternResult keyPatternResult) {
        final DataRegion dataRegion = recordReader.getDataRegion();
        final KeyPatternResult existingKeyPattern =
                recordReader.getKeyPattern(keyPatternResult.getName());
        if (null != existingKeyPattern) {
            if (!keyPatternResult.equals(existingKeyPattern)) {
                final String msg = String.format(
                        "Key pattern %s already exist with different key(s): %s",
                        existingKeyPattern.getName(),
                        existingKeyPattern.getKeyPattern());
                return Collections.singletonList(ValidationError.error(dataRegion, msg));
            }
        }
        final List<ValidationError> errors = new ArrayList<>();
        final List<String> keys = keyPatternResult.getKeyPattern();
        for (String key : keys) {
            final KeyResult keyResult = getNewOrExistingKey(key);
            if (null == keyResult) {
                final String message = String.format(
                        "Key %s specified in the key_pattern %s doesn't exist",
                        key,
                        keyPatternResult.getName());
                errors.add(ValidationError.error(dataRegion, message));
                continue;
            }
            final String definingTable = keyResult.getDefiningTableName();
            final TableResult definingTableResult = getNewOrExistingTable(definingTable);
            if (null == definingTableResult) {
                final String message = String.format("The defining table %s for %s doesn't exist",
                        definingTable,
                        key);
                errors.add(ValidationError.error(dataRegion, message));
                continue;
            }
            final String keyPatternName = definingTableResult.getKeyPattern();
            final KeyPatternResult definingKeyPatternResult =
                    getNewOrExistingKeyPattern(keyPatternName);
            if (null == definingKeyPatternResult) {
                final String message = String.format(
                        "Key pattern: %s which is a dependency of key pattern: %s does not exist.",
                        keyPatternName,
                        keyPatternResult.getName());
                errors.add(ValidationError.error(dataRegion, message));
                continue;
            }
            final List<String> definingTableKeys = definingKeyPatternResult.getKeyPattern();
            if (!keys.containsAll(definingTableKeys)) {
                final List<String> missing = missing(definingTableKeys, keys);
                final String message = String.format(
                        "Key pattern %s is missing a dependant key(s): %s",
                        keyPatternResult.getName(),
                        missing);
                errors.add(ValidationError.error(dataRegion, message));
            }
        }
        return errors;
    }

    /**
     * Returns a {@link List} of all the elements which are in {@code a} but not in {@code b}.
     *
     * @param a list a
     * @param b list b
     *
     * @return {@link List} of all the elements which are in {@code a} but not in {@code b}.
     */
    @Nonnull
    private static <T> List<T> missing(@Nonnull final List<T> a, @Nonnull final List<T> b) {
        List<T> aClone = new ArrayList<>(a);
        aClone.removeAll(b);
        return Collections.unmodifiableList(aClone);
    }

    /**
     * Returns the {@link TableResult} in the following order:
     * <ol>
     *      <li>From the {@link TableDefinitions} associated with this instance.</li>
     *      <li>From G2S2. </li>
     *      <li>Otherwise returns {@code null}</li>
     * </ol>
     *
     * @param tableName the table name.
     *
     * @return The {@link TableResult} from the specified {@code tableName} if one exists in the
     * {@link TableDefinitions} or in G2S2. Otherwise, returns null.
     */
    @CheckForNull
    private TableResult getNewOrExistingTable(@Nonnull final String tableName) {
        final TableResult n = tableDefinitions.getTable(tableName);
        if (null != n) {
            return n;
        }
        return recordReader.getTable(tableName);
    }

    /**
     * Returns the {@link KeyResult} in the following order:
     * <ol>
     *      <li>From the {@link TableDefinitions} associated with this instance.</li>
     *      <li>From G2S2. </li>
     *      <li>Otherwise returns {@code null}</li>
     * </ol>
     *
     * @param keyName the key name.
     *
     * @return The {@link KeyResult} from the specified {@code tableName} if one exists in the
     * {@link TableDefinitions} or in G2S2. Otherwise, returns null.
     */
    @CheckForNull
    private KeyResult getNewOrExistingKey(@Nonnull final String keyName) {
        final KeyResult n = tableDefinitions.getKey(keyName);
        if (null != n) {
            return n;
        }
        return recordReader.getKey(keyName);
    }

    /**
     * Returns the {@link KeyPatternResult} in the following order:
     * <ol>
     *      <li>From the {@link TableDefinitions} associated with this instance.</li>
     *      <li>From G2S2. </li>
     *      <li>Otherwise returns {@code null}</li>
     * </ol>
     *
     * @param keyPatternName the key pattern name.
     *
     * @return The {@link KeyPatternResult} from the specified {@code tableName} if one exists in
     * the {@link TableDefinitions} or in G2S2. Otherwise, returns null.
     */
    @CheckForNull
    private KeyPatternResult getNewOrExistingKeyPattern(@Nonnull final String keyPatternName) {
        final KeyPatternResult n = tableDefinitions.getKeyPattern(keyPatternName);
        if (null != n) {
            return n;
        }
        return recordReader.getKeyPattern(keyPatternName);
    }
}