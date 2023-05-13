#pragma once

#include <yt/yt/client/table_client/row_base.h>

#include <yt/yt/library/codegen/builder_base.h>

namespace NYT::NQueryClient::NDetail {

struct TStringView;
struct TFieldId;
struct TValidatorPersistentState;

////////////////////////////////////////////////////////////////////////////////

llvm::Value* GetLogicalTypeMax(NCodegen::TIRBuilderBase& builder, NTableClient::ESimpleLogicalValueType type);

llvm::Value* GetLogicalTypeMin(NCodegen::TIRBuilderBase& builder, NTableClient::ESimpleLogicalValueType type);

constexpr NYson::EYsonItemType ExpectedYsonItemType(NTableClient::EValueType physicalType);

const NTableClient::TLogicalTypePtr& UnwrapTaggedAndOptional(const NTableClient::TLogicalTypePtr& type);

////////////////////////////////////////////////////////////////////////////////

TString GetDescription(NTableClient::TComplexTypeFieldDescriptor* rootDescriptor, TFieldId* fieldId);

template <class TErrorCode>
[[noreturn]] void ThrowParseError(
    TErrorCode errorCode,
    const char* errorDescription,
    TFieldId* fieldId,
    NTableClient::TComplexTypeFieldDescriptor* rootDescriptor);

[[noreturn]] void ThrowUnexpectedYsonToken(
    NYson::TYsonPullParserCursor* cursor,
    NYson::EYsonItemType type,
    TFieldId* fieldId,
    NTableClient::TComplexTypeFieldDescriptor* rootDescriptor);

template <class TNumber>
[[noreturn]] void ThrowOutOfRange(
    TNumber value,
    TNumber min,
    TNumber max,
    TFieldId* fieldId,
    NTableClient::TComplexTypeFieldDescriptor* rootDescriptor);

[[noreturn]] void ThrowBadVariantAlternative(
    i64 alternative,
    i64 alternativesCount,
    TFieldId* fieldId,
    NTableClient::TComplexTypeFieldDescriptor* rootDescriptor);

[[noreturn]] void ThrowMissingRequiredField(
    const char* fieldName,
    TFieldId* fieldId,
    NTableClient::TComplexTypeFieldDescriptor* rootDescriptor);

[[noreturn]] void ThrowNullRequiredColumn(
    const char* columnName,
    NTableClient::EValueType valueType);

[[noreturn]] void ThrowBadColumnValue(
    const char* columnName,
    NTableClient::EValueType valueType,
    NTableClient::EValueType expectedType);

NYson::EYsonItemType Cursor_GetCurrent_Type(NYson::TYsonPullParserCursor* cursor);
void Cursor_Next(NYson::TYsonPullParserCursor* cursor);
void Cursor_SkipComplexValue(NYson::TYsonPullParserCursor* cursor);
i8 Cursor_UncheckedAsBoolean(NYson::TYsonPullParserCursor* cursor);
i64 Cursor_UncheckedAsInt64(NYson::TYsonPullParserCursor* cursor);
ui64 Cursor_UncheckedAsUint64(NYson::TYsonPullParserCursor* cursor);
double Cursor_UncheckedAsDouble(NYson::TYsonPullParserCursor* cursor);
TStringView Cursor_UncheckedAsStringView(NYson::TYsonPullParserCursor* cursor);

void ValidateDecimalValue(
    TStringView binaryValue,
    int precision,
    int scale,
    TFieldId* fieldId,
    NTableClient::TComplexTypeFieldDescriptor* rootDescriptor);

void ValidateYsonValue(TStringView yson);

void ValidateJsonValue(
    TStringView value,
    TFieldId* fieldId,
    NTableClient::TComplexTypeFieldDescriptor* rootDescriptor);

void InitYsonParser(TValidatorPersistentState* state, TStringView yson);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient::NDetail

#define HELPERS_INL_H_
#include "helpers-inl.h"
#undef HELPERS_INL_H_
