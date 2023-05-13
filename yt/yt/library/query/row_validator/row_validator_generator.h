#pragma once

#include <yt/yt/core/yson/pull_parser.h>

#include <yt/yt/client/table_client/schema.h>

#include <util/stream/mem.h>

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

typedef void(IRowValidatorImplSignature)(const NTableClient::TUnversionedValue*);

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

struct TFieldId
{
    const TFieldId* Parent = nullptr;
    ui64 SiblingIndex = 0;

    NTableClient::TComplexTypeFieldDescriptor GetDescriptor(const NTableClient::TComplexTypeFieldDescriptor& root) const;
};

struct TStringView {
    const char* Data = nullptr;
    ui64 Length = 0;
};

struct TValidatorPersistentState
{
    // TODO(aleexfi): Migrate to pointers to field names from schema isntead of global strings.
    NTableClient::TTableSchemaPtr Schema;
    std::vector<NTableClient::TComplexTypeFieldDescriptor> FieldDescriptors;
    TMemoryInput MemoryInput;
    std::optional<NYson::TYsonPullParser> Parser;
    std::optional<NYson::TYsonPullParserCursor> Cursor;

    TValidatorPersistentState();
};

////////////////////////////////////////////////////////////////////////////////

constexpr ui64 DateUpperBound = 49673ull;
constexpr ui64 DatetimeUpperBound = DateUpperBound * 86400ull;
constexpr ui64 TimestampUpperBound = DatetimeUpperBound * 1000000ull;

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
