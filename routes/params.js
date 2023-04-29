export const matchIdParam = {
  name: "match_id",
  in: "path",
  required: true,
  type: "integer",
};
export const accountIdParam = {
  name: "account_id",
  in: "path",
  description: "Steam32 account ID",
  required: true,
  type: "integer",
};
export const teamIdPathParam = {
  name: "team_id",
  in: "path",
  description: "Team ID",
  required: true,
  type: "integer",
};
export const leagueIdPathParam = {
  name: "league_id",
  in: "path",
  description: "League ID",
  required: true,
  type: "integer",
};
export const heroIdPathParam = {
  name: "hero_id",
  in: "path",
  description: "Hero ID",
  required: true,
  type: "integer",
};
export const fieldParam = {
  name: "field",
  in: "path",
  description: "Field to aggregate on",
  required: true,
  type: "string",
};
export const limitParam = {
  name: "limit",
  in: "query",
  description: "Number of matches to limit to",
  required: false,
  type: "integer",
};
export const offsetParam = {
  name: "offset",
  in: "query",
  description: "Number of matches to offset start by",
  required: false,
  type: "integer",
};
export const projectParam = {
  name: "project",
  in: "query",
  description: "Fields to project (array)",
  required: false,
  type: "string",
};
export const winParam = {
  name: "win",
  in: "query",
  description: "Whether the player won",
  required: false,
  type: "integer",
};
export const patchParam = {
  name: "patch",
  in: "query",
  description: "Patch ID",
  required: false,
  type: "integer",
};
export const gameModeParam = {
  name: "game_mode",
  in: "query",
  description: "Game Mode ID",
  required: false,
  type: "integer",
};
export const lobbyTypeParam = {
  name: "lobby_type",
  in: "query",
  description: "Lobby type ID",
  required: false,
  type: "integer",
};
export const regionParam = {
  name: "region",
  in: "query",
  description: "Region ID",
  required: false,
  type: "integer",
};
export const dateParam = {
  name: "date",
  in: "query",
  description: "Days previous",
  required: false,
  type: "integer",
};
export const laneRoleParam = {
  name: "lane_role",
  in: "query",
  description: "Lane Role ID",
  required: false,
  type: "integer",
};
export const heroIdParam = {
  name: "hero_id",
  in: "query",
  description: "Hero ID",
  required: false,
  type: "integer",
};
export const isRadiantParam = {
  name: "is_radiant",
  in: "query",
  description: "Whether the player was radiant",
  required: false,
  type: "integer",
};
export const withHeroIdParam = {
  name: "with_hero_id",
  in: "query",
  description: "Hero IDs on the player's team (array)",
  required: false,
  type: "integer",
};
export const againstHeroIdParam = {
  name: "against_hero_id",
  in: "query",
  description: "Hero IDs against the player's team (array)",
  required: false,
  type: "integer",
};
export const withAccountIdParam = {
  name: "with_account_id",
  in: "query",
  description: "Account IDs on the player's team (array)",
  required: false,
  type: "integer",
};
export const againstAccountIdParam = {
  name: "against_account_id",
  in: "query",
  description: "Account IDs against the player's team (array)",
  required: false,
  type: "integer",
};
export const includedAccountIdParam = {
  name: "included_account_id",
  in: "query",
  description: "Account IDs in the match (array)",
  required: false,
  type: "integer",
};
export const excludedAccountIdParam = {
  name: "excluded_account_id",
  in: "query",
  description: "Account IDs not in the match (array)",
  required: false,
  type: "integer",
};
export const significantParam = {
  name: "significant",
  in: "query",
  description: "Whether the match was significant for aggregation purposes. Defaults to 1 (true), set this to 0 to return data for non-standard modes/matches.",
  required: false,
  type: "integer",
};
export const sortParam = {
  name: "sort",
  in: "query",
  description: "The field to return matches sorted by in descending order",
  required: false,
  type: "string",
};
export const havingParam = {
  name: "having",
  in: "query",
  description: "The minimum number of games played, for filtering hero stats",
  required: false,
  type: "integer",
};
export const minMmrParam = {
  name: "min_mmr",
  in: "query",
  description: "Minimum average MMR",
  required: false,
  type: "integer",
};
export const maxMmrParam = {
  name: "max_mmr",
  in: "query",
  description: "Maximum average MMR",
  required: false,
  type: "integer",
};
export const minTimeParam = {
  name: "min_time",
  in: "query",
  description: "Minimum start time (Unix time)",
  required: false,
  type: "integer",
};
export const maxTimeParam = {
  name: "max_time",
  in: "query",
  description: "Maximum start time (Unix time)",
  required: false,
  type: "integer",
};
export const mmrAscendingParam = {
  name: "mmr_ascending",
  in: "query",
  description: "Order by MMR ascending",
  required: false,
  type: "integer",
};
export const mmrDescendingParam = {
  name: "mmr_descending",
  in: "query",
  description: "Order by MMR descending",
  required: false,
  type: "integer",
};
export const lessThanMatchIdParam = {
  name: "less_than_match_id",
  in: "query",
  description: "Get matches with a match ID lower than this value",
  required: false,
  type: "integer",
};
export const matchOverviewParam = {
  name: "overview",
  in: "query",
  description: "Only fetch data required for match overview page",
  required: false,
  type: "integer",
};
