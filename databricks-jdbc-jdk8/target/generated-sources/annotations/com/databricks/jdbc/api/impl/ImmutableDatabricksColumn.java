package com.databricks.jdbc.api.impl;

import com.databricks.jdbc.common.AccessType;
import com.google.common.base.MoreObjects;
import com.google.common.primitives.Booleans;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.errorprone.annotations.Var;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import javax.annotation.CheckReturnValue;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link DatabricksColumn}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableDatabricksColumn.builder()}.
 */
@Generated(from = "DatabricksColumn", generator = "Immutables")
@SuppressWarnings({"all"})
@SuppressFBWarnings
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
@CheckReturnValue
public final class ImmutableDatabricksColumn implements DatabricksColumn {
  private final String columnName;
  private final int columnType;
  private final String columnTypeText;
  private final int typePrecision;
  private final int displaySize;
  private final boolean isSigned;
  private final @Nullable String schemaName;
  private final boolean isCurrency;
  private final boolean isAutoIncrement;
  private final boolean isCaseSensitive;
  private final boolean isSearchable;
  private final com.databricks.jdbc.common.Nullable nullable;
  private final int typeScale;
  private final AccessType accessType;
  private final boolean isDefinitelyWritable;
  private final String columnTypeClassName;
  private final @Nullable String tableName;
  private final String catalogName;

  private ImmutableDatabricksColumn(
      String columnName,
      int columnType,
      String columnTypeText,
      int typePrecision,
      int displaySize,
      boolean isSigned,
      @Nullable String schemaName,
      boolean isCurrency,
      boolean isAutoIncrement,
      boolean isCaseSensitive,
      boolean isSearchable,
      com.databricks.jdbc.common.Nullable nullable,
      int typeScale,
      AccessType accessType,
      boolean isDefinitelyWritable,
      String columnTypeClassName,
      @Nullable String tableName,
      String catalogName) {
    this.columnName = columnName;
    this.columnType = columnType;
    this.columnTypeText = columnTypeText;
    this.typePrecision = typePrecision;
    this.displaySize = displaySize;
    this.isSigned = isSigned;
    this.schemaName = schemaName;
    this.isCurrency = isCurrency;
    this.isAutoIncrement = isAutoIncrement;
    this.isCaseSensitive = isCaseSensitive;
    this.isSearchable = isSearchable;
    this.nullable = nullable;
    this.typeScale = typeScale;
    this.accessType = accessType;
    this.isDefinitelyWritable = isDefinitelyWritable;
    this.columnTypeClassName = columnTypeClassName;
    this.tableName = tableName;
    this.catalogName = catalogName;
  }

  /**
   *Name of the column in result set 
   */
  @Override
  public String columnName() {
    return columnName;
  }

  /**
   *Type of the column in result set 
   */
  @Override
  public int columnType() {
    return columnType;
  }

  /**
   *Full data type spec, SQL/catalogString text 
   */
  @Override
  public String columnTypeText() {
    return columnTypeText;
  }

  /**
   * Precision is the maximum number of significant digits that can be stored in a column. For
   * string, it's 255.
   */
  @Override
  public int typePrecision() {
    return typePrecision;
  }

  /**
   * @return The value of the {@code displaySize} attribute
   */
  @Override
  public int displaySize() {
    return displaySize;
  }

  /**
   * @return The value of the {@code isSigned} attribute
   */
  @Override
  public boolean isSigned() {
    return isSigned;
  }

  /**
   * @return The value of the {@code schemaName} attribute
   */
  @Override
  public @Nullable String schemaName() {
    return schemaName;
  }

  /**
   * @return The value of the {@code isCurrency} attribute
   */
  @Override
  public boolean isCurrency() {
    return isCurrency;
  }

  /**
   * @return The value of the {@code isAutoIncrement} attribute
   */
  @Override
  public boolean isAutoIncrement() {
    return isAutoIncrement;
  }

  /**
   * @return The value of the {@code isCaseSensitive} attribute
   */
  @Override
  public boolean isCaseSensitive() {
    return isCaseSensitive;
  }

  /**
   * @return The value of the {@code isSearchable} attribute
   */
  @Override
  public boolean isSearchable() {
    return isSearchable;
  }

  /**
   * @return The value of the {@code nullable} attribute
   */
  @Override
  public com.databricks.jdbc.common.Nullable nullable() {
    return nullable;
  }

  /**
   * @return The value of the {@code typeScale} attribute
   */
  @Override
  public int typeScale() {
    return typeScale;
  }

  /**
   * @return The value of the {@code accessType} attribute
   */
  @Override
  public AccessType accessType() {
    return accessType;
  }

  /**
   * @return The value of the {@code isDefinitelyWritable} attribute
   */
  @Override
  public boolean isDefinitelyWritable() {
    return isDefinitelyWritable;
  }

  /**
   * @return The value of the {@code columnTypeClassName} attribute
   */
  @Override
  public String columnTypeClassName() {
    return columnTypeClassName;
  }

  /**
   * @return The value of the {@code tableName} attribute
   */
  @Override
  public @Nullable String tableName() {
    return tableName;
  }

  /**
   * @return The value of the {@code catalogName} attribute
   */
  @Override
  public String catalogName() {
    return catalogName;
  }

  /**
   * Copy the current immutable object by setting a value for the {@link DatabricksColumn#columnName() columnName} attribute.
   * An equals check used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for columnName
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableDatabricksColumn withColumnName(String value) {
    String newValue = Objects.requireNonNull(value, "columnName");
    if (this.columnName.equals(newValue)) return this;
    return new ImmutableDatabricksColumn(
        newValue,
        this.columnType,
        this.columnTypeText,
        this.typePrecision,
        this.displaySize,
        this.isSigned,
        this.schemaName,
        this.isCurrency,
        this.isAutoIncrement,
        this.isCaseSensitive,
        this.isSearchable,
        this.nullable,
        this.typeScale,
        this.accessType,
        this.isDefinitelyWritable,
        this.columnTypeClassName,
        this.tableName,
        this.catalogName);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link DatabricksColumn#columnType() columnType} attribute.
   * A value equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for columnType
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableDatabricksColumn withColumnType(int value) {
    if (this.columnType == value) return this;
    return new ImmutableDatabricksColumn(
        this.columnName,
        value,
        this.columnTypeText,
        this.typePrecision,
        this.displaySize,
        this.isSigned,
        this.schemaName,
        this.isCurrency,
        this.isAutoIncrement,
        this.isCaseSensitive,
        this.isSearchable,
        this.nullable,
        this.typeScale,
        this.accessType,
        this.isDefinitelyWritable,
        this.columnTypeClassName,
        this.tableName,
        this.catalogName);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link DatabricksColumn#columnTypeText() columnTypeText} attribute.
   * An equals check used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for columnTypeText
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableDatabricksColumn withColumnTypeText(String value) {
    String newValue = Objects.requireNonNull(value, "columnTypeText");
    if (this.columnTypeText.equals(newValue)) return this;
    return new ImmutableDatabricksColumn(
        this.columnName,
        this.columnType,
        newValue,
        this.typePrecision,
        this.displaySize,
        this.isSigned,
        this.schemaName,
        this.isCurrency,
        this.isAutoIncrement,
        this.isCaseSensitive,
        this.isSearchable,
        this.nullable,
        this.typeScale,
        this.accessType,
        this.isDefinitelyWritable,
        this.columnTypeClassName,
        this.tableName,
        this.catalogName);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link DatabricksColumn#typePrecision() typePrecision} attribute.
   * A value equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for typePrecision
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableDatabricksColumn withTypePrecision(int value) {
    if (this.typePrecision == value) return this;
    return new ImmutableDatabricksColumn(
        this.columnName,
        this.columnType,
        this.columnTypeText,
        value,
        this.displaySize,
        this.isSigned,
        this.schemaName,
        this.isCurrency,
        this.isAutoIncrement,
        this.isCaseSensitive,
        this.isSearchable,
        this.nullable,
        this.typeScale,
        this.accessType,
        this.isDefinitelyWritable,
        this.columnTypeClassName,
        this.tableName,
        this.catalogName);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link DatabricksColumn#displaySize() displaySize} attribute.
   * A value equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for displaySize
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableDatabricksColumn withDisplaySize(int value) {
    if (this.displaySize == value) return this;
    return new ImmutableDatabricksColumn(
        this.columnName,
        this.columnType,
        this.columnTypeText,
        this.typePrecision,
        value,
        this.isSigned,
        this.schemaName,
        this.isCurrency,
        this.isAutoIncrement,
        this.isCaseSensitive,
        this.isSearchable,
        this.nullable,
        this.typeScale,
        this.accessType,
        this.isDefinitelyWritable,
        this.columnTypeClassName,
        this.tableName,
        this.catalogName);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link DatabricksColumn#isSigned() isSigned} attribute.
   * A value equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for isSigned
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableDatabricksColumn withIsSigned(boolean value) {
    if (this.isSigned == value) return this;
    return new ImmutableDatabricksColumn(
        this.columnName,
        this.columnType,
        this.columnTypeText,
        this.typePrecision,
        this.displaySize,
        value,
        this.schemaName,
        this.isCurrency,
        this.isAutoIncrement,
        this.isCaseSensitive,
        this.isSearchable,
        this.nullable,
        this.typeScale,
        this.accessType,
        this.isDefinitelyWritable,
        this.columnTypeClassName,
        this.tableName,
        this.catalogName);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link DatabricksColumn#schemaName() schemaName} attribute.
   * An equals check used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for schemaName (can be {@code null})
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableDatabricksColumn withSchemaName(@Nullable String value) {
    if (Objects.equals(this.schemaName, value)) return this;
    return new ImmutableDatabricksColumn(
        this.columnName,
        this.columnType,
        this.columnTypeText,
        this.typePrecision,
        this.displaySize,
        this.isSigned,
        value,
        this.isCurrency,
        this.isAutoIncrement,
        this.isCaseSensitive,
        this.isSearchable,
        this.nullable,
        this.typeScale,
        this.accessType,
        this.isDefinitelyWritable,
        this.columnTypeClassName,
        this.tableName,
        this.catalogName);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link DatabricksColumn#isCurrency() isCurrency} attribute.
   * A value equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for isCurrency
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableDatabricksColumn withIsCurrency(boolean value) {
    if (this.isCurrency == value) return this;
    return new ImmutableDatabricksColumn(
        this.columnName,
        this.columnType,
        this.columnTypeText,
        this.typePrecision,
        this.displaySize,
        this.isSigned,
        this.schemaName,
        value,
        this.isAutoIncrement,
        this.isCaseSensitive,
        this.isSearchable,
        this.nullable,
        this.typeScale,
        this.accessType,
        this.isDefinitelyWritable,
        this.columnTypeClassName,
        this.tableName,
        this.catalogName);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link DatabricksColumn#isAutoIncrement() isAutoIncrement} attribute.
   * A value equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for isAutoIncrement
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableDatabricksColumn withIsAutoIncrement(boolean value) {
    if (this.isAutoIncrement == value) return this;
    return new ImmutableDatabricksColumn(
        this.columnName,
        this.columnType,
        this.columnTypeText,
        this.typePrecision,
        this.displaySize,
        this.isSigned,
        this.schemaName,
        this.isCurrency,
        value,
        this.isCaseSensitive,
        this.isSearchable,
        this.nullable,
        this.typeScale,
        this.accessType,
        this.isDefinitelyWritable,
        this.columnTypeClassName,
        this.tableName,
        this.catalogName);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link DatabricksColumn#isCaseSensitive() isCaseSensitive} attribute.
   * A value equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for isCaseSensitive
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableDatabricksColumn withIsCaseSensitive(boolean value) {
    if (this.isCaseSensitive == value) return this;
    return new ImmutableDatabricksColumn(
        this.columnName,
        this.columnType,
        this.columnTypeText,
        this.typePrecision,
        this.displaySize,
        this.isSigned,
        this.schemaName,
        this.isCurrency,
        this.isAutoIncrement,
        value,
        this.isSearchable,
        this.nullable,
        this.typeScale,
        this.accessType,
        this.isDefinitelyWritable,
        this.columnTypeClassName,
        this.tableName,
        this.catalogName);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link DatabricksColumn#isSearchable() isSearchable} attribute.
   * A value equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for isSearchable
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableDatabricksColumn withIsSearchable(boolean value) {
    if (this.isSearchable == value) return this;
    return new ImmutableDatabricksColumn(
        this.columnName,
        this.columnType,
        this.columnTypeText,
        this.typePrecision,
        this.displaySize,
        this.isSigned,
        this.schemaName,
        this.isCurrency,
        this.isAutoIncrement,
        this.isCaseSensitive,
        value,
        this.nullable,
        this.typeScale,
        this.accessType,
        this.isDefinitelyWritable,
        this.columnTypeClassName,
        this.tableName,
        this.catalogName);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link DatabricksColumn#nullable() nullable} attribute.
   * A value equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for nullable
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableDatabricksColumn withNullable(com.databricks.jdbc.common.Nullable value) {
    com.databricks.jdbc.common.Nullable newValue = Objects.requireNonNull(value, "nullable");
    if (this.nullable == newValue) return this;
    return new ImmutableDatabricksColumn(
        this.columnName,
        this.columnType,
        this.columnTypeText,
        this.typePrecision,
        this.displaySize,
        this.isSigned,
        this.schemaName,
        this.isCurrency,
        this.isAutoIncrement,
        this.isCaseSensitive,
        this.isSearchable,
        newValue,
        this.typeScale,
        this.accessType,
        this.isDefinitelyWritable,
        this.columnTypeClassName,
        this.tableName,
        this.catalogName);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link DatabricksColumn#typeScale() typeScale} attribute.
   * A value equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for typeScale
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableDatabricksColumn withTypeScale(int value) {
    if (this.typeScale == value) return this;
    return new ImmutableDatabricksColumn(
        this.columnName,
        this.columnType,
        this.columnTypeText,
        this.typePrecision,
        this.displaySize,
        this.isSigned,
        this.schemaName,
        this.isCurrency,
        this.isAutoIncrement,
        this.isCaseSensitive,
        this.isSearchable,
        this.nullable,
        value,
        this.accessType,
        this.isDefinitelyWritable,
        this.columnTypeClassName,
        this.tableName,
        this.catalogName);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link DatabricksColumn#accessType() accessType} attribute.
   * A value equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for accessType
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableDatabricksColumn withAccessType(AccessType value) {
    AccessType newValue = Objects.requireNonNull(value, "accessType");
    if (this.accessType == newValue) return this;
    return new ImmutableDatabricksColumn(
        this.columnName,
        this.columnType,
        this.columnTypeText,
        this.typePrecision,
        this.displaySize,
        this.isSigned,
        this.schemaName,
        this.isCurrency,
        this.isAutoIncrement,
        this.isCaseSensitive,
        this.isSearchable,
        this.nullable,
        this.typeScale,
        newValue,
        this.isDefinitelyWritable,
        this.columnTypeClassName,
        this.tableName,
        this.catalogName);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link DatabricksColumn#isDefinitelyWritable() isDefinitelyWritable} attribute.
   * A value equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for isDefinitelyWritable
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableDatabricksColumn withIsDefinitelyWritable(boolean value) {
    if (this.isDefinitelyWritable == value) return this;
    return new ImmutableDatabricksColumn(
        this.columnName,
        this.columnType,
        this.columnTypeText,
        this.typePrecision,
        this.displaySize,
        this.isSigned,
        this.schemaName,
        this.isCurrency,
        this.isAutoIncrement,
        this.isCaseSensitive,
        this.isSearchable,
        this.nullable,
        this.typeScale,
        this.accessType,
        value,
        this.columnTypeClassName,
        this.tableName,
        this.catalogName);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link DatabricksColumn#columnTypeClassName() columnTypeClassName} attribute.
   * An equals check used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for columnTypeClassName
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableDatabricksColumn withColumnTypeClassName(String value) {
    String newValue = Objects.requireNonNull(value, "columnTypeClassName");
    if (this.columnTypeClassName.equals(newValue)) return this;
    return new ImmutableDatabricksColumn(
        this.columnName,
        this.columnType,
        this.columnTypeText,
        this.typePrecision,
        this.displaySize,
        this.isSigned,
        this.schemaName,
        this.isCurrency,
        this.isAutoIncrement,
        this.isCaseSensitive,
        this.isSearchable,
        this.nullable,
        this.typeScale,
        this.accessType,
        this.isDefinitelyWritable,
        newValue,
        this.tableName,
        this.catalogName);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link DatabricksColumn#tableName() tableName} attribute.
   * An equals check used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for tableName (can be {@code null})
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableDatabricksColumn withTableName(@Nullable String value) {
    if (Objects.equals(this.tableName, value)) return this;
    return new ImmutableDatabricksColumn(
        this.columnName,
        this.columnType,
        this.columnTypeText,
        this.typePrecision,
        this.displaySize,
        this.isSigned,
        this.schemaName,
        this.isCurrency,
        this.isAutoIncrement,
        this.isCaseSensitive,
        this.isSearchable,
        this.nullable,
        this.typeScale,
        this.accessType,
        this.isDefinitelyWritable,
        this.columnTypeClassName,
        value,
        this.catalogName);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link DatabricksColumn#catalogName() catalogName} attribute.
   * An equals check used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for catalogName
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableDatabricksColumn withCatalogName(String value) {
    String newValue = Objects.requireNonNull(value, "catalogName");
    if (this.catalogName.equals(newValue)) return this;
    return new ImmutableDatabricksColumn(
        this.columnName,
        this.columnType,
        this.columnTypeText,
        this.typePrecision,
        this.displaySize,
        this.isSigned,
        this.schemaName,
        this.isCurrency,
        this.isAutoIncrement,
        this.isCaseSensitive,
        this.isSearchable,
        this.nullable,
        this.typeScale,
        this.accessType,
        this.isDefinitelyWritable,
        this.columnTypeClassName,
        this.tableName,
        newValue);
  }

  /**
   * This instance is equal to all instances of {@code ImmutableDatabricksColumn} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableDatabricksColumn
        && equalTo(0, (ImmutableDatabricksColumn) another);
  }

  private boolean equalTo(int synthetic, ImmutableDatabricksColumn another) {
    return columnName.equals(another.columnName)
        && columnType == another.columnType
        && columnTypeText.equals(another.columnTypeText)
        && typePrecision == another.typePrecision
        && displaySize == another.displaySize
        && isSigned == another.isSigned
        && Objects.equals(schemaName, another.schemaName)
        && isCurrency == another.isCurrency
        && isAutoIncrement == another.isAutoIncrement
        && isCaseSensitive == another.isCaseSensitive
        && isSearchable == another.isSearchable
        && nullable.equals(another.nullable)
        && typeScale == another.typeScale
        && accessType.equals(another.accessType)
        && isDefinitelyWritable == another.isDefinitelyWritable
        && columnTypeClassName.equals(another.columnTypeClassName)
        && Objects.equals(tableName, another.tableName)
        && catalogName.equals(another.catalogName);
  }

  /**
   * Computes a hash code from attributes: {@code columnName}, {@code columnType}, {@code columnTypeText}, {@code typePrecision}, {@code displaySize}, {@code isSigned}, {@code schemaName}, {@code isCurrency}, {@code isAutoIncrement}, {@code isCaseSensitive}, {@code isSearchable}, {@code nullable}, {@code typeScale}, {@code accessType}, {@code isDefinitelyWritable}, {@code columnTypeClassName}, {@code tableName}, {@code catalogName}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    @Var int h = 5381;
    h += (h << 5) + columnName.hashCode();
    h += (h << 5) + columnType;
    h += (h << 5) + columnTypeText.hashCode();
    h += (h << 5) + typePrecision;
    h += (h << 5) + displaySize;
    h += (h << 5) + Booleans.hashCode(isSigned);
    h += (h << 5) + Objects.hashCode(schemaName);
    h += (h << 5) + Booleans.hashCode(isCurrency);
    h += (h << 5) + Booleans.hashCode(isAutoIncrement);
    h += (h << 5) + Booleans.hashCode(isCaseSensitive);
    h += (h << 5) + Booleans.hashCode(isSearchable);
    h += (h << 5) + nullable.hashCode();
    h += (h << 5) + typeScale;
    h += (h << 5) + accessType.hashCode();
    h += (h << 5) + Booleans.hashCode(isDefinitelyWritable);
    h += (h << 5) + columnTypeClassName.hashCode();
    h += (h << 5) + Objects.hashCode(tableName);
    h += (h << 5) + catalogName.hashCode();
    return h;
  }

  /**
   * Prints the immutable value {@code DatabricksColumn} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return MoreObjects.toStringHelper("DatabricksColumn")
        .omitNullValues()
        .add("columnName", columnName)
        .add("columnType", columnType)
        .add("columnTypeText", columnTypeText)
        .add("typePrecision", typePrecision)
        .add("displaySize", displaySize)
        .add("isSigned", isSigned)
        .add("schemaName", schemaName)
        .add("isCurrency", isCurrency)
        .add("isAutoIncrement", isAutoIncrement)
        .add("isCaseSensitive", isCaseSensitive)
        .add("isSearchable", isSearchable)
        .add("nullable", nullable)
        .add("typeScale", typeScale)
        .add("accessType", accessType)
        .add("isDefinitelyWritable", isDefinitelyWritable)
        .add("columnTypeClassName", columnTypeClassName)
        .add("tableName", tableName)
        .add("catalogName", catalogName)
        .toString();
  }

  /**
   * Creates an immutable copy of a {@link DatabricksColumn} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable DatabricksColumn instance
   */
  public static ImmutableDatabricksColumn copyOf(DatabricksColumn instance) {
    if (instance instanceof ImmutableDatabricksColumn) {
      return (ImmutableDatabricksColumn) instance;
    }
    return ImmutableDatabricksColumn.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableDatabricksColumn ImmutableDatabricksColumn}.
   * <pre>
   * ImmutableDatabricksColumn.builder()
   *    .columnName(String) // required {@link DatabricksColumn#columnName() columnName}
   *    .columnType(int) // required {@link DatabricksColumn#columnType() columnType}
   *    .columnTypeText(String) // required {@link DatabricksColumn#columnTypeText() columnTypeText}
   *    .typePrecision(int) // required {@link DatabricksColumn#typePrecision() typePrecision}
   *    .displaySize(int) // required {@link DatabricksColumn#displaySize() displaySize}
   *    .isSigned(boolean) // required {@link DatabricksColumn#isSigned() isSigned}
   *    .schemaName(String | null) // nullable {@link DatabricksColumn#schemaName() schemaName}
   *    .isCurrency(boolean) // required {@link DatabricksColumn#isCurrency() isCurrency}
   *    .isAutoIncrement(boolean) // required {@link DatabricksColumn#isAutoIncrement() isAutoIncrement}
   *    .isCaseSensitive(boolean) // required {@link DatabricksColumn#isCaseSensitive() isCaseSensitive}
   *    .isSearchable(boolean) // required {@link DatabricksColumn#isSearchable() isSearchable}
   *    .nullable(com.databricks.jdbc.common.Nullable) // required {@link DatabricksColumn#nullable() nullable}
   *    .typeScale(int) // required {@link DatabricksColumn#typeScale() typeScale}
   *    .accessType(com.databricks.jdbc.common.AccessType) // required {@link DatabricksColumn#accessType() accessType}
   *    .isDefinitelyWritable(boolean) // required {@link DatabricksColumn#isDefinitelyWritable() isDefinitelyWritable}
   *    .columnTypeClassName(String) // required {@link DatabricksColumn#columnTypeClassName() columnTypeClassName}
   *    .tableName(String | null) // nullable {@link DatabricksColumn#tableName() tableName}
   *    .catalogName(String) // required {@link DatabricksColumn#catalogName() catalogName}
   *    .build();
   * </pre>
   * @return A new ImmutableDatabricksColumn builder
   */
  public static ImmutableDatabricksColumn.Builder builder() {
    return new ImmutableDatabricksColumn.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableDatabricksColumn ImmutableDatabricksColumn}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "DatabricksColumn", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder {
    private static final long INIT_BIT_COLUMN_NAME = 0x1L;
    private static final long INIT_BIT_COLUMN_TYPE = 0x2L;
    private static final long INIT_BIT_COLUMN_TYPE_TEXT = 0x4L;
    private static final long INIT_BIT_TYPE_PRECISION = 0x8L;
    private static final long INIT_BIT_DISPLAY_SIZE = 0x10L;
    private static final long INIT_BIT_IS_SIGNED = 0x20L;
    private static final long INIT_BIT_IS_CURRENCY = 0x40L;
    private static final long INIT_BIT_IS_AUTO_INCREMENT = 0x80L;
    private static final long INIT_BIT_IS_CASE_SENSITIVE = 0x100L;
    private static final long INIT_BIT_IS_SEARCHABLE = 0x200L;
    private static final long INIT_BIT_NULLABLE = 0x400L;
    private static final long INIT_BIT_TYPE_SCALE = 0x800L;
    private static final long INIT_BIT_ACCESS_TYPE = 0x1000L;
    private static final long INIT_BIT_IS_DEFINITELY_WRITABLE = 0x2000L;
    private static final long INIT_BIT_COLUMN_TYPE_CLASS_NAME = 0x4000L;
    private static final long INIT_BIT_CATALOG_NAME = 0x8000L;
    private long initBits = 0xffffL;

    private @Nullable String columnName;
    private int columnType;
    private @Nullable String columnTypeText;
    private int typePrecision;
    private int displaySize;
    private boolean isSigned;
    private @Nullable String schemaName;
    private boolean isCurrency;
    private boolean isAutoIncrement;
    private boolean isCaseSensitive;
    private boolean isSearchable;
    private @Nullable com.databricks.jdbc.common.Nullable nullable;
    private int typeScale;
    private @Nullable AccessType accessType;
    private boolean isDefinitelyWritable;
    private @Nullable String columnTypeClassName;
    private @Nullable String tableName;
    private @Nullable String catalogName;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code DatabricksColumn} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder from(DatabricksColumn instance) {
      Objects.requireNonNull(instance, "instance");
      columnName(instance.columnName());
      columnType(instance.columnType());
      columnTypeText(instance.columnTypeText());
      typePrecision(instance.typePrecision());
      displaySize(instance.displaySize());
      isSigned(instance.isSigned());
      @Nullable String schemaNameValue = instance.schemaName();
      if (schemaNameValue != null) {
        schemaName(schemaNameValue);
      }
      isCurrency(instance.isCurrency());
      isAutoIncrement(instance.isAutoIncrement());
      isCaseSensitive(instance.isCaseSensitive());
      isSearchable(instance.isSearchable());
      nullable(instance.nullable());
      typeScale(instance.typeScale());
      accessType(instance.accessType());
      isDefinitelyWritable(instance.isDefinitelyWritable());
      columnTypeClassName(instance.columnTypeClassName());
      @Nullable String tableNameValue = instance.tableName();
      if (tableNameValue != null) {
        tableName(tableNameValue);
      }
      catalogName(instance.catalogName());
      return this;
    }

    /**
     * Initializes the value for the {@link DatabricksColumn#columnName() columnName} attribute.
     * @param columnName The value for columnName 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder columnName(String columnName) {
      this.columnName = Objects.requireNonNull(columnName, "columnName");
      initBits &= ~INIT_BIT_COLUMN_NAME;
      return this;
    }

    /**
     * Initializes the value for the {@link DatabricksColumn#columnType() columnType} attribute.
     * @param columnType The value for columnType 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder columnType(int columnType) {
      this.columnType = columnType;
      initBits &= ~INIT_BIT_COLUMN_TYPE;
      return this;
    }

    /**
     * Initializes the value for the {@link DatabricksColumn#columnTypeText() columnTypeText} attribute.
     * @param columnTypeText The value for columnTypeText 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder columnTypeText(String columnTypeText) {
      this.columnTypeText = Objects.requireNonNull(columnTypeText, "columnTypeText");
      initBits &= ~INIT_BIT_COLUMN_TYPE_TEXT;
      return this;
    }

    /**
     * Initializes the value for the {@link DatabricksColumn#typePrecision() typePrecision} attribute.
     * @param typePrecision The value for typePrecision 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder typePrecision(int typePrecision) {
      this.typePrecision = typePrecision;
      initBits &= ~INIT_BIT_TYPE_PRECISION;
      return this;
    }

    /**
     * Initializes the value for the {@link DatabricksColumn#displaySize() displaySize} attribute.
     * @param displaySize The value for displaySize 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder displaySize(int displaySize) {
      this.displaySize = displaySize;
      initBits &= ~INIT_BIT_DISPLAY_SIZE;
      return this;
    }

    /**
     * Initializes the value for the {@link DatabricksColumn#isSigned() isSigned} attribute.
     * @param isSigned The value for isSigned 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder isSigned(boolean isSigned) {
      this.isSigned = isSigned;
      initBits &= ~INIT_BIT_IS_SIGNED;
      return this;
    }

    /**
     * Initializes the value for the {@link DatabricksColumn#schemaName() schemaName} attribute.
     * @param schemaName The value for schemaName (can be {@code null})
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder schemaName(@Nullable String schemaName) {
      this.schemaName = schemaName;
      return this;
    }

    /**
     * Initializes the value for the {@link DatabricksColumn#isCurrency() isCurrency} attribute.
     * @param isCurrency The value for isCurrency 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder isCurrency(boolean isCurrency) {
      this.isCurrency = isCurrency;
      initBits &= ~INIT_BIT_IS_CURRENCY;
      return this;
    }

    /**
     * Initializes the value for the {@link DatabricksColumn#isAutoIncrement() isAutoIncrement} attribute.
     * @param isAutoIncrement The value for isAutoIncrement 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder isAutoIncrement(boolean isAutoIncrement) {
      this.isAutoIncrement = isAutoIncrement;
      initBits &= ~INIT_BIT_IS_AUTO_INCREMENT;
      return this;
    }

    /**
     * Initializes the value for the {@link DatabricksColumn#isCaseSensitive() isCaseSensitive} attribute.
     * @param isCaseSensitive The value for isCaseSensitive 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder isCaseSensitive(boolean isCaseSensitive) {
      this.isCaseSensitive = isCaseSensitive;
      initBits &= ~INIT_BIT_IS_CASE_SENSITIVE;
      return this;
    }

    /**
     * Initializes the value for the {@link DatabricksColumn#isSearchable() isSearchable} attribute.
     * @param isSearchable The value for isSearchable 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder isSearchable(boolean isSearchable) {
      this.isSearchable = isSearchable;
      initBits &= ~INIT_BIT_IS_SEARCHABLE;
      return this;
    }

    /**
     * Initializes the value for the {@link DatabricksColumn#nullable() nullable} attribute.
     * @param nullable The value for nullable 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder nullable(com.databricks.jdbc.common.Nullable nullable) {
      this.nullable = Objects.requireNonNull(nullable, "nullable");
      initBits &= ~INIT_BIT_NULLABLE;
      return this;
    }

    /**
     * Initializes the value for the {@link DatabricksColumn#typeScale() typeScale} attribute.
     * @param typeScale The value for typeScale 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder typeScale(int typeScale) {
      this.typeScale = typeScale;
      initBits &= ~INIT_BIT_TYPE_SCALE;
      return this;
    }

    /**
     * Initializes the value for the {@link DatabricksColumn#accessType() accessType} attribute.
     * @param accessType The value for accessType 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder accessType(AccessType accessType) {
      this.accessType = Objects.requireNonNull(accessType, "accessType");
      initBits &= ~INIT_BIT_ACCESS_TYPE;
      return this;
    }

    /**
     * Initializes the value for the {@link DatabricksColumn#isDefinitelyWritable() isDefinitelyWritable} attribute.
     * @param isDefinitelyWritable The value for isDefinitelyWritable 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder isDefinitelyWritable(boolean isDefinitelyWritable) {
      this.isDefinitelyWritable = isDefinitelyWritable;
      initBits &= ~INIT_BIT_IS_DEFINITELY_WRITABLE;
      return this;
    }

    /**
     * Initializes the value for the {@link DatabricksColumn#columnTypeClassName() columnTypeClassName} attribute.
     * @param columnTypeClassName The value for columnTypeClassName 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder columnTypeClassName(String columnTypeClassName) {
      this.columnTypeClassName = Objects.requireNonNull(columnTypeClassName, "columnTypeClassName");
      initBits &= ~INIT_BIT_COLUMN_TYPE_CLASS_NAME;
      return this;
    }

    /**
     * Initializes the value for the {@link DatabricksColumn#tableName() tableName} attribute.
     * @param tableName The value for tableName (can be {@code null})
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder tableName(@Nullable String tableName) {
      this.tableName = tableName;
      return this;
    }

    /**
     * Initializes the value for the {@link DatabricksColumn#catalogName() catalogName} attribute.
     * @param catalogName The value for catalogName 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder catalogName(String catalogName) {
      this.catalogName = Objects.requireNonNull(catalogName, "catalogName");
      initBits &= ~INIT_BIT_CATALOG_NAME;
      return this;
    }

    /**
     * Builds a new {@link ImmutableDatabricksColumn ImmutableDatabricksColumn}.
     * @return An immutable instance of DatabricksColumn
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableDatabricksColumn build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return new ImmutableDatabricksColumn(
          columnName,
          columnType,
          columnTypeText,
          typePrecision,
          displaySize,
          isSigned,
          schemaName,
          isCurrency,
          isAutoIncrement,
          isCaseSensitive,
          isSearchable,
          nullable,
          typeScale,
          accessType,
          isDefinitelyWritable,
          columnTypeClassName,
          tableName,
          catalogName);
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_COLUMN_NAME) != 0) attributes.add("columnName");
      if ((initBits & INIT_BIT_COLUMN_TYPE) != 0) attributes.add("columnType");
      if ((initBits & INIT_BIT_COLUMN_TYPE_TEXT) != 0) attributes.add("columnTypeText");
      if ((initBits & INIT_BIT_TYPE_PRECISION) != 0) attributes.add("typePrecision");
      if ((initBits & INIT_BIT_DISPLAY_SIZE) != 0) attributes.add("displaySize");
      if ((initBits & INIT_BIT_IS_SIGNED) != 0) attributes.add("isSigned");
      if ((initBits & INIT_BIT_IS_CURRENCY) != 0) attributes.add("isCurrency");
      if ((initBits & INIT_BIT_IS_AUTO_INCREMENT) != 0) attributes.add("isAutoIncrement");
      if ((initBits & INIT_BIT_IS_CASE_SENSITIVE) != 0) attributes.add("isCaseSensitive");
      if ((initBits & INIT_BIT_IS_SEARCHABLE) != 0) attributes.add("isSearchable");
      if ((initBits & INIT_BIT_NULLABLE) != 0) attributes.add("nullable");
      if ((initBits & INIT_BIT_TYPE_SCALE) != 0) attributes.add("typeScale");
      if ((initBits & INIT_BIT_ACCESS_TYPE) != 0) attributes.add("accessType");
      if ((initBits & INIT_BIT_IS_DEFINITELY_WRITABLE) != 0) attributes.add("isDefinitelyWritable");
      if ((initBits & INIT_BIT_COLUMN_TYPE_CLASS_NAME) != 0) attributes.add("columnTypeClassName");
      if ((initBits & INIT_BIT_CATALOG_NAME) != 0) attributes.add("catalogName");
      return "Cannot build DatabricksColumn, some of required attributes are not set " + attributes;
    }
  }
}
