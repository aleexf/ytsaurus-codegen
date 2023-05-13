#include "helpers.h"

#include "row_validator_generator.h"

#include <yt/yt/client/table_client/logical_type.h>

#include <yt/yt/library/codegen/builder_base.h>

#include <yt/yt/library/decimal/decimal.h>

#include <yt/yt/core/yson/parser.h>

#include <library/cpp/yt/yson/consumer.h>

#include <library/cpp/json/common/defs.h>
#include <library/cpp/json/json_reader.h>

namespace NYT::NQueryClient::NDetail {

using namespace llvm;
using namespace NYson;
using namespace NJson;
using namespace NCodegen;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

Value* GetLogicalTypeMax(TIRBuilderBase& builder, ESimpleLogicalValueType type)
{
    // Ints
    if (type == ESimpleLogicalValueType::Int8) {
        return builder.ConstantIntValue<i64>(Max<i8>());
    } else if (type == ESimpleLogicalValueType::Int16) {
        return builder.ConstantIntValue<i64>(Max<i16>());
    } else if (type == ESimpleLogicalValueType::Int32) {
        return builder.ConstantIntValue<i64>(Max<i32>());
    } else if (type == ESimpleLogicalValueType::Int64) {
        return builder.ConstantIntValue<i64>(Max<i64>());
    } else if (type == ESimpleLogicalValueType::Uint8) { // Uints
        return builder.ConstantIntValue<ui64>(Max<ui8>());
    } else if (type == ESimpleLogicalValueType::Uint16) {
        return builder.ConstantIntValue<ui64>(Max<ui16>());
    } else if (type == ESimpleLogicalValueType::Uint32) {
        return builder.ConstantIntValue<ui64>(Max<ui32>());
    } else if (type == ESimpleLogicalValueType::Uint64) {
        return builder.ConstantIntValue<ui64>(Max<ui64>());
    } else if (type == ESimpleLogicalValueType::Date) { // Time types
        return builder.ConstantIntValue<ui64>(DateUpperBound - 1);
    } else if (type == ESimpleLogicalValueType::Datetime) {
        return builder.ConstantIntValue<ui64>(DatetimeUpperBound - 1);
    } else if (type == ESimpleLogicalValueType::Timestamp) {
        return builder.ConstantIntValue<ui64>(TimestampUpperBound - 1);
    } else if (type == ESimpleLogicalValueType::Interval) {
        return builder.ConstantIntValue<i64>(TimestampUpperBound - 1);
    } else if (type == ESimpleLogicalValueType::Float) {
        return ConstantFP::get(TTypeBuilder<double>::Get(builder.Context), Max<float>());
    } else {
        YT_VERIFY(false && "unsupported type");
    }
    Y_UNREACHABLE();
}


Value* GetLogicalTypeMin(TIRBuilderBase& builder, ESimpleLogicalValueType type)
{
    // Ints
    if (type == ESimpleLogicalValueType::Int8) {
        return builder.ConstantIntValue<i64>(Min<i8>());
    } else if (type == ESimpleLogicalValueType::Int16) {
        return builder.ConstantIntValue<i64>(Min<i16>());
    } else if (type == ESimpleLogicalValueType::Int32) {
        return builder.ConstantIntValue<i64>(Min<i32>());
    } else if (type == ESimpleLogicalValueType::Int64) {
        return builder.ConstantIntValue<i64>(Min<i64>());
    } else if (type == ESimpleLogicalValueType::Uint8) { // Uints
        return builder.ConstantIntValue<ui64>(Min<ui8>());
    } else if (type == ESimpleLogicalValueType::Uint16) {
        return builder.ConstantIntValue<ui64>(Min<ui16>());
    } else if (type == ESimpleLogicalValueType::Uint32) {
        return builder.ConstantIntValue<ui64>(Min<ui32>());
    } else if (type == ESimpleLogicalValueType::Uint64) {
        return builder.ConstantIntValue<ui64>(Min<ui64>());
    } else if (type == ESimpleLogicalValueType::Date) { // Time types
        return builder.ConstantIntValue<ui64>(0);
    } else if (type == ESimpleLogicalValueType::Datetime) {
        return builder.ConstantIntValue<ui64>(0);
    } else if (type == ESimpleLogicalValueType::Timestamp) {
        return builder.ConstantIntValue<ui64>(0);
    } else if (type == ESimpleLogicalValueType::Interval) {
        return builder.ConstantIntValue<i64>(-TimestampUpperBound + 1);
    } else if (type == ESimpleLogicalValueType::Float) {
        return ConstantFP::get(TTypeBuilder<double>::Get(builder.Context), std::numeric_limits<float>::lowest());
    } else {
        YT_VERIFY(false && "unsupported type");
    }
    Y_UNREACHABLE();
}

const TLogicalTypePtr& UnwrapTaggedAndOptional(const TLogicalTypePtr& type)
{
    const TLogicalTypePtr* current = &type;
    while ((*current)->GetMetatype() == ELogicalMetatype::Tagged) {
        current = &(*current)->UncheckedAsTaggedTypeRef().GetElement();
    }

    if ((*current)->GetMetatype() != ELogicalMetatype::Optional) {
        return *current;
    }

    const auto& optionalType = (*current)->UncheckedAsOptionalTypeRef();
    if (optionalType.IsElementNullable()) {
        return *current;
    }

    current = &optionalType.GetElement();

    while ((*current)->GetMetatype() == ELogicalMetatype::Tagged) {
        current = &(*current)->UncheckedAsTaggedTypeRef().GetElement();
    }

    return *current;
}

////////////////////////////////////////////////////////////////////////////////

TString GetDescription(TComplexTypeFieldDescriptor* rootDescriptor, TFieldId* fieldId)
{
    return fieldId->GetDescriptor(*rootDescriptor).GetDescription();
}

[[noreturn]] void ThrowUnexpectedYsonToken(
    TYsonPullParserCursor* cursor,
    EYsonItemType type,
    TFieldId* fieldId,
    TComplexTypeFieldDescriptor* rootDescriptor)
{
    THROW_ERROR_EXCEPTION(NTableClient::EErrorCode::SchemaViolation,
        "Cannot parse %Qv: expected %Qlv, found %Qlv",
        GetDescription(rootDescriptor, fieldId),
        type,
        cursor->GetCurrent().GetType());
}

[[noreturn]] void ThrowBadVariantAlternative(
    i64 alternative,
    i64 alternativesCount,
    TFieldId* fieldId,
    TComplexTypeFieldDescriptor* rootDescriptor)
{
    THROW_ERROR_EXCEPTION(NTableClient::EErrorCode::SchemaViolation,
        "Cannot parse %Qv; variant alternative index %v is out of range [0, %v)",
        GetDescription(rootDescriptor, fieldId),
        alternative,
        alternativesCount);
}

[[noreturn]] void ThrowMissingRequiredField(
    const char* fieldName,
    TFieldId* fieldId,
    TComplexTypeFieldDescriptor* rootDescriptor)
{
    THROW_ERROR_EXCEPTION(NTableClient::EErrorCode::SchemaViolation,
        "Cannot parse %Qv; struct ended before required field %Qv is set",
        GetDescription(rootDescriptor, fieldId),
        fieldName);
}

[[noreturn]] void ThrowNullRequiredColumn(
    const char* columnName,
    EValueType valueType)
{
    THROW_ERROR_EXCEPTION(NTableClient::EErrorCode::SchemaViolation,
        "Required column %v cannot have %Qlv value",
        columnName,
        valueType);
}

[[noreturn]] void ThrowBadColumnValue(
    const char* columnName,
    EValueType valueType,
    EValueType expectedType)
{
    THROW_ERROR_EXCEPTION(
        NTableClient::EErrorCode::SchemaViolation,
        "Column %v cannot have %Qlv value, expected value type: %Qlv",
        columnName,
        valueType,
        expectedType);
}

EYsonItemType Cursor_GetCurrent_Type(TYsonPullParserCursor* cursor)
{
    return cursor->GetCurrent().GetType();
}

void Cursor_Next(TYsonPullParserCursor* cursor)
{
    cursor->Next();
}

void Cursor_SkipComplexValue(TYsonPullParserCursor* cursor)
{
    cursor->SkipComplexValue();
}

i8 Cursor_UncheckedAsBoolean(TYsonPullParserCursor* cursor)
{
    return cursor->GetCurrent().UncheckedAsBoolean();
}

i64 Cursor_UncheckedAsInt64(TYsonPullParserCursor* cursor)
{
    return cursor->GetCurrent().UncheckedAsInt64();
}

ui64 Cursor_UncheckedAsUint64(TYsonPullParserCursor* cursor)
{
    return cursor->GetCurrent().UncheckedAsUint64();
}

double Cursor_UncheckedAsDouble(TYsonPullParserCursor* cursor)
{
    return cursor->GetCurrent().UncheckedAsDouble();
}

TStringView Cursor_UncheckedAsStringView(TYsonPullParserCursor* cursor)
{
    auto str = cursor->GetCurrent().UncheckedAsString();
    return TStringView{
        .Data = str.data(),
        .Length = str.length()
    };
}

void ValidateDecimalValue(
    TStringView binaryValue,
    int precision,
    int scale,
    TFieldId* fieldId,
    TComplexTypeFieldDescriptor* rootDescriptor)
{
    try {
        NDecimal::TDecimal::ValidateBinaryValue(
            TStringBuf(binaryValue.Data, binaryValue.Length),
            precision,
            scale);
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION(NTableClient::EErrorCode::SchemaViolation,
            "Error validating field %Qv",
            GetDescription(rootDescriptor, fieldId))
            << ex;
    }
}

class TYsonAnyValidator
    : public NYson::IYsonConsumer
{
public:
    void OnStringScalar(TStringBuf /*value*/) override
    { }

    void OnInt64Scalar(i64 /*value*/) override
    { }

    void OnUint64Scalar(ui64 /*value*/) override
    { }

    void OnDoubleScalar(double /*value*/) override
    { }

    void OnBooleanScalar(bool /*value*/) override
    { }

    void OnEntity() override
    { }

    void OnBeginList() override
    {
        ++Depth_;
    }

    void OnListItem() override
    { }

    void OnEndList() override
    {
        --Depth_;
    }

    void OnBeginMap() override
    {
        ++Depth_;
    }

    void OnKeyedItem(TStringBuf /*key*/) override
    { }

    void OnEndMap() override
    {
        --Depth_;
    }

    void OnBeginAttributes() override
    {
        if (Depth_ == 0) {
            THROW_ERROR_EXCEPTION("Table values cannot have top-level attributes");
        }
    }

    void OnEndAttributes() override
    { }

    void OnRaw(TStringBuf /*yson*/, EYsonType /*type*/) override
    { }

private:
    int Depth_ = 0;
};

void ValidateYsonValue(TStringView yson)
{
    TYsonAnyValidator validator;
    NYson::ParseYsonStringBuffer({yson.Data, yson.Length}, EYsonType::Node, &validator);
}

class TValidateJsonCallbacks
    : public TJsonCallbacks
{
public:
    TValidateJsonCallbacks()
        : TJsonCallbacks(/* throwOnError */ true)
    {}

    bool OnDouble(double value) final
    {
        if (Y_UNLIKELY(std::isinf(value))) {
            ythrow TJsonException() << "infinite values are not allowed";
        }
        return true;
    }

    bool OnEnd() final
    {
        if (Finished_) {
            ythrow TJsonException() << "JSON value is already finished";
        }
        Finished_ = true;
        return true;
    }

private:
     bool Finished_ = false;
};

void ValidateJsonValue(TStringView value, TFieldId* fieldId, TComplexTypeFieldDescriptor* rootDescriptor)
{
    TMemoryInput input(TStringBuf(value.Data, value.Length));
    TValidateJsonCallbacks callbacks;
    try {
        auto ok = ReadJson(&input, &callbacks);
        // We expect all the errors to be thrown.
        YT_VERIFY(ok);
    } catch (const TJsonException& ex) {
        THROW_ERROR_EXCEPTION(NTableClient::EErrorCode::SchemaViolation,
            "Cannot parse %Qv: Invalid JSON",
            GetDescription(rootDescriptor, fieldId))
            << ex;
    }
}

void InitYsonParser(TValidatorPersistentState* state, TStringView yson)
{
    state->MemoryInput = TMemoryInput(TStringBuf(yson.Data, yson.Length));
    state->Parser.emplace(&state->MemoryInput, EYsonType::Node);
    state->Cursor.emplace(&*state->Parser);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient::NDetail
