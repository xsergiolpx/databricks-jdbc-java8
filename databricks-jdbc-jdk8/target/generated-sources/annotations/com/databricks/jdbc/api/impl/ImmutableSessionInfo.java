package com.databricks.jdbc.api.impl;

import com.databricks.jdbc.common.IDatabricksComputeResource;
import com.databricks.jdbc.model.client.thrift.generated.TSessionHandle;
import com.google.common.base.MoreObjects;
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
 * Immutable implementation of {@link SessionInfo}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableSessionInfo.builder()}.
 */
@Generated(from = "SessionInfo", generator = "Immutables")
@SuppressWarnings({"all"})
@SuppressFBWarnings
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
@CheckReturnValue
public final class ImmutableSessionInfo implements SessionInfo {
  private final String sessionId;
  private final IDatabricksComputeResource computeResource;
  private final @Nullable TSessionHandle sessionHandle;

  private ImmutableSessionInfo(
      String sessionId,
      IDatabricksComputeResource computeResource,
      @Nullable TSessionHandle sessionHandle) {
    this.sessionId = sessionId;
    this.computeResource = computeResource;
    this.sessionHandle = sessionHandle;
  }

  /**
   * @return The value of the {@code sessionId} attribute
   */
  @Override
  public String sessionId() {
    return sessionId;
  }

  /**
   * @return The value of the {@code computeResource} attribute
   */
  @Override
  public IDatabricksComputeResource computeResource() {
    return computeResource;
  }

  /**
   * @return The value of the {@code sessionHandle} attribute
   */
  @Override
  public @Nullable TSessionHandle sessionHandle() {
    return sessionHandle;
  }

  /**
   * Copy the current immutable object by setting a value for the {@link SessionInfo#sessionId() sessionId} attribute.
   * An equals check used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for sessionId
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableSessionInfo withSessionId(String value) {
    String newValue = Objects.requireNonNull(value, "sessionId");
    if (this.sessionId.equals(newValue)) return this;
    return new ImmutableSessionInfo(newValue, this.computeResource, this.sessionHandle);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link SessionInfo#computeResource() computeResource} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for computeResource
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableSessionInfo withComputeResource(IDatabricksComputeResource value) {
    if (this.computeResource == value) return this;
    IDatabricksComputeResource newValue = Objects.requireNonNull(value, "computeResource");
    return new ImmutableSessionInfo(this.sessionId, newValue, this.sessionHandle);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link SessionInfo#sessionHandle() sessionHandle} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for sessionHandle (can be {@code null})
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableSessionInfo withSessionHandle(@Nullable TSessionHandle value) {
    if (this.sessionHandle == value) return this;
    return new ImmutableSessionInfo(this.sessionId, this.computeResource, value);
  }

  /**
   * This instance is equal to all instances of {@code ImmutableSessionInfo} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableSessionInfo
        && equalTo(0, (ImmutableSessionInfo) another);
  }

  private boolean equalTo(int synthetic, ImmutableSessionInfo another) {
    return sessionId.equals(another.sessionId)
        && computeResource.equals(another.computeResource)
        && Objects.equals(sessionHandle, another.sessionHandle);
  }

  /**
   * Computes a hash code from attributes: {@code sessionId}, {@code computeResource}, {@code sessionHandle}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    @Var int h = 5381;
    h += (h << 5) + sessionId.hashCode();
    h += (h << 5) + computeResource.hashCode();
    h += (h << 5) + Objects.hashCode(sessionHandle);
    return h;
  }

  /**
   * Prints the immutable value {@code SessionInfo} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return MoreObjects.toStringHelper("SessionInfo")
        .omitNullValues()
        .add("sessionId", sessionId)
        .add("computeResource", computeResource)
        .add("sessionHandle", sessionHandle)
        .toString();
  }

  /**
   * Creates an immutable copy of a {@link SessionInfo} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable SessionInfo instance
   */
  public static ImmutableSessionInfo copyOf(SessionInfo instance) {
    if (instance instanceof ImmutableSessionInfo) {
      return (ImmutableSessionInfo) instance;
    }
    return ImmutableSessionInfo.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableSessionInfo ImmutableSessionInfo}.
   * <pre>
   * ImmutableSessionInfo.builder()
   *    .sessionId(String) // required {@link SessionInfo#sessionId() sessionId}
   *    .computeResource(com.databricks.jdbc.common.IDatabricksComputeResource) // required {@link SessionInfo#computeResource() computeResource}
   *    .sessionHandle(com.databricks.jdbc.model.client.thrift.generated.TSessionHandle | null) // nullable {@link SessionInfo#sessionHandle() sessionHandle}
   *    .build();
   * </pre>
   * @return A new ImmutableSessionInfo builder
   */
  public static ImmutableSessionInfo.Builder builder() {
    return new ImmutableSessionInfo.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableSessionInfo ImmutableSessionInfo}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "SessionInfo", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder {
    private static final long INIT_BIT_SESSION_ID = 0x1L;
    private static final long INIT_BIT_COMPUTE_RESOURCE = 0x2L;
    private long initBits = 0x3L;

    private @Nullable String sessionId;
    private @Nullable IDatabricksComputeResource computeResource;
    private @Nullable TSessionHandle sessionHandle;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code SessionInfo} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder from(SessionInfo instance) {
      Objects.requireNonNull(instance, "instance");
      sessionId(instance.sessionId());
      computeResource(instance.computeResource());
      @Nullable TSessionHandle sessionHandleValue = instance.sessionHandle();
      if (sessionHandleValue != null) {
        sessionHandle(sessionHandleValue);
      }
      return this;
    }

    /**
     * Initializes the value for the {@link SessionInfo#sessionId() sessionId} attribute.
     * @param sessionId The value for sessionId 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder sessionId(String sessionId) {
      this.sessionId = Objects.requireNonNull(sessionId, "sessionId");
      initBits &= ~INIT_BIT_SESSION_ID;
      return this;
    }

    /**
     * Initializes the value for the {@link SessionInfo#computeResource() computeResource} attribute.
     * @param computeResource The value for computeResource 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder computeResource(IDatabricksComputeResource computeResource) {
      this.computeResource = Objects.requireNonNull(computeResource, "computeResource");
      initBits &= ~INIT_BIT_COMPUTE_RESOURCE;
      return this;
    }

    /**
     * Initializes the value for the {@link SessionInfo#sessionHandle() sessionHandle} attribute.
     * @param sessionHandle The value for sessionHandle (can be {@code null})
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder sessionHandle(@Nullable TSessionHandle sessionHandle) {
      this.sessionHandle = sessionHandle;
      return this;
    }

    /**
     * Builds a new {@link ImmutableSessionInfo ImmutableSessionInfo}.
     * @return An immutable instance of SessionInfo
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableSessionInfo build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return new ImmutableSessionInfo(sessionId, computeResource, sessionHandle);
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_SESSION_ID) != 0) attributes.add("sessionId");
      if ((initBits & INIT_BIT_COMPUTE_RESOURCE) != 0) attributes.add("computeResource");
      return "Cannot build SessionInfo, some of required attributes are not set " + attributes;
    }
  }
}
