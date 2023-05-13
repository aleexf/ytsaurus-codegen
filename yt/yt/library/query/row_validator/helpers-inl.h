#ifndef HELPERS_INL_H_
#error "Direct inclusion of this file is not allowed, include validate_logical_type.h"
// For the sake of sane code completion.
#include "helpers.h"
#endif

#include <yt/yt/core/yson/syntax_checker.h>

namespace NYT::NQueryClient::NDetail {

////////////////////////////////////////////////////////////////////////////////

constexpr NYson::EYsonItemType ExpectedYsonItemType(NTableClient::EValueType physicalType)
{
    using NYson::EYsonItemType;
    using NTableClient::EValueType;

    switch (physicalType) {
        case EValueType::Boolean:
            return EYsonItemType::BooleanValue;
        case EValueType::Int64:
            return EYsonItemType::Int64Value;
        case EValueType::Uint64:
            return EYsonItemType::Uint64Value;
        case EValueType::Double:
            return EYsonItemType::DoubleValue;
        case EValueType::String:
            return EYsonItemType::StringValue;
        case EValueType::Null:
            return EYsonItemType::EntityValue;
        default:
            Y_FAIL("Unexpected physical type");
    }
}

template <class TErrorCode>
[[noreturn]] void ThrowParseError(
    TErrorCode errorCode,
    const char* errorDescription,
    TFieldId* fieldId,
    NTableClient::TComplexTypeFieldDescriptor* rootDescriptor)
{
    THROW_ERROR_EXCEPTION(errorCode,
        "Cannot parse %Qv: %v",
        GetDescription(rootDescriptor, fieldId),
        errorDescription);
}

template <class TNumber>
[[noreturn]] void ThrowOutOfRange(
    TNumber value,
    TNumber min,
    TNumber max,
    TFieldId* fieldId,
    NTableClient::TComplexTypeFieldDescriptor* rootDescriptor)
{
    THROW_ERROR_EXCEPTION(
        NTableClient::EErrorCode::SchemaViolation,
        "Cannot parse %Qv: Value %v is out of allowed range [%v, %v]",
        GetDescription(rootDescriptor, fieldId),
        value,
        min,
        max);
}


////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient::NDetail
