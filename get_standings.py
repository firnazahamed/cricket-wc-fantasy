import os
import pandas as pd
import numpy as np

from helpers import upload_df_to_gcs, retrieve_scorecards
from settings import weeks, owner_team_dict, player_id_dict


def retrieve_team_info():
    sheet_titles = [
        c for c in os.listdir("./Squads/") if not c.startswith(".")
    ]  # Ignore hidden files like .DS_Store
    weekly_dicts = {}
    squad_df = pd.DataFrame()
    for sheet_title in sheet_titles:

        players_df = pd.read_csv(f"./Squads/{sheet_title}")
        squad_df = pd.concat([squad_df, players_df], ignore_index=True)
        captain_dict = {}
        playing_11_dict = {}
        reserve_dict = {}
        for owner in players_df.columns:
            captain_dict[owner] = players_df[owner][0]
            playing_11_dict[owner] = players_df[owner][:11].values
            reserve_dict[owner] = players_df[owner][11:15].values

        sheet_name = sheet_title.split(".csv")[0]
        weekly_dicts[sheet_name] = {
            "playing_11_dict": playing_11_dict,
            "reserve_dict": reserve_dict,
            "captain_dict": captain_dict,
        }

    squad_dict = {
        col: list(set(squad_df[col][squad_df[col].notnull()].values))
        for col in squad_df.columns
    }

    return weekly_dicts, squad_dict


def create_score_df(
    scorecards, weekly_dicts, squad_dict, weeks, owner_team_dict, player_id_dict
):

    score_df = pd.DataFrame(columns=["Owner", "Player"])
    for k, v in squad_dict.items():
        for player in v:
            score_df.loc[len(score_df.index)] = [k, player]

    score_df["Player_id"] = [
        int(player_id_dict[player.strip()]) for player in score_df["Player"]
    ]

    match_ids = [sc.split("_")[0] for sc in scorecards.keys()]
    for match_id in match_ids:
        for k, v in weeks.items():
            if match_id in v["matches"]:
                week = k

        scorecard = scorecards[match_id + "_scorecard"].astype({"total_points": "int"})

        owners = np.array(
            [[k] * 11 for k in weekly_dicts[week]["playing_11_dict"].keys()]
        ).flatten()
        players = np.array(
            list(weekly_dicts[week]["playing_11_dict"].values())
        ).flatten()
        playing_df = pd.DataFrame(data={"Owner": owners, "Player": players})
        playing_df["Player_id"] = [
            int(player_id_dict[player.strip()]) for player in playing_df["Player"]
        ]

        playing_df = (
            playing_df.merge(scorecard[["Player_id", "total_points"]], on="Player_id")
            # .reset_index()
        )

        ## 1.5x points for captain
        playing_df.loc[
            playing_df["Player"].isin(weekly_dicts[week]["captain_dict"].values()),
            "total_points",
        ] = (
            playing_df.loc[
                playing_df["Player"].isin(weekly_dicts[week]["captain_dict"].values())
            ]["total_points"]
            .apply(lambda x: np.ceil(x * 1.5))
            .astype(int)
        )

        # Reserve players points
        reserve_owners = np.array(
            [[k] * 4 for k in weekly_dicts[week]["reserve_dict"].keys()]
        ).flatten()
        reserve_players = np.array(
            list(weekly_dicts[week]["reserve_dict"].values())
        ).flatten()
        reserve_df = pd.DataFrame(
            data={"Owner": reserve_owners, "Player": reserve_players}
        ).fillna("")
        reserve_df["Player_id"] = [
            int(player_id_dict[player.strip()])
            if player is not None and player != ""
            else None
            for player in reserve_df["Player"]
        ]

        reserve_df = (
            reserve_df.set_index("Player_id")
            .join(scorecard[["Player_id", "total_points"]].set_index("Player_id"))
            .reset_index()
        )
        reserve_df["total_points"] = round(reserve_df["total_points"] / 2)

        points_df = pd.concat([playing_df, reserve_df])
        score_df = score_df.merge(
            points_df, on=["Owner", "Player_id", "Player"], how="left"
        ).rename(columns={"total_points": match_id + "_points"})
        score_df = (
            score_df.loc[:, ~score_df.columns.duplicated()].drop_duplicates().fillna("")
        )

    game_cols = [col for col in score_df.columns if col.endswith("_points")]
    game_map = {
        game_cols[game]: "Match_" + str(game + 1) for game in range(len(game_cols))
    }
    score_df = score_df.rename(columns=game_map)

    sum_df = (
        score_df.replace(r"^\s*$", 0, regex=True)
        .groupby("Owner")
        .agg({c: "sum" for c in score_df.columns if c.startswith("Match")})
    )

    cumsum_df = sum_df.cumsum(axis=1)
    standings_df = sum_df.sum(axis=1).to_frame(name="Points")
    standings_df["Standings"] = standings_df["Points"].rank(ascending=False)
    standings_df = standings_df.sort_values("Standings")
    standings_df.insert(
        0, "Team", [owner_team_dict[owner] for owner in standings_df.index.values]
    )

    weekly_points_df = score_df[["Owner", "Player"]]
    for week in weeks.keys():
        scores_available = list(
            set(
                [
                    game_map[m + "_points"]
                    for m in weeks[week]["matches"]
                    if m + "_points" in game_map.keys()
                ]
            ).intersection(set(score_df.columns))
        )
        if len(scores_available) > 0:
            weekly_points_df[week + "_points"] = (
                score_df[scores_available].replace(r"^\s*$", 0, regex=True).sum(axis=1)
            )

    combined_scorecards = pd.concat([scorecards[k] for k in scorecards.keys()])
    agg_points_df = (
        combined_scorecards.groupby(["Name_batting"])
        .agg(
            {
                "batting_points": "sum",
                "bowling_points": "sum",
                "fielding_points": "sum",
                "total_points": "sum",
            }
        )
        .reset_index()
        .sort_values("total_points", ascending=False)
    )

    return (
        score_df,
        sum_df.reset_index(),
        cumsum_df.reset_index(),
        standings_df.reset_index(),
        weekly_points_df,
        agg_points_df,
    )


def save_outputs(
    score_df, sum_df, cumsum_df, standings_df, weekly_points_df, agg_points_df
):

    # Save output files locally
    score_df.to_csv("./Outputs/score_df.csv", header=True, index=False)
    sum_df.to_csv("./Outputs/sum_df.csv", header=True, index=False)
    cumsum_df.to_csv("./Outputs/cumsum_df.csv", header=True, index=False)
    standings_df.to_csv("./Outputs/standings_df.csv", header=True, index=False)
    weekly_points_df.to_csv("./Outputs/weekly_points_df.csv", header=True, index=False)
    agg_points_df.to_csv("./Outputs/agg_points_df.csv", header=True, index=False)

    # Save output files to GCS
    bucket_name = "wc-2023"
    upload_df_to_gcs(score_df, f"Outputs/score_df.csv", bucket_name)
    upload_df_to_gcs(sum_df, f"Outputs/sum_df.csv", bucket_name)
    upload_df_to_gcs(cumsum_df, f"Outputs/cumsum_df.csv", bucket_name)
    upload_df_to_gcs(standings_df, f"Outputs/standings_df.csv", bucket_name)
    upload_df_to_gcs(weekly_points_df, f"Outputs/weekly_points_df.csv", bucket_name)
    upload_df_to_gcs(agg_points_df, f"Outputs/agg_points_df.csv", bucket_name)


if __name__ == "__main__":

    scorecards = retrieve_scorecards()
    weekly_dicts, squad_dict = retrieve_team_info()
    (
        score_df,
        sum_df,
        cumsum_df,
        standings_df,
        weekly_points_df,
        agg_points_df,
    ) = create_score_df(
        scorecards, weekly_dicts, squad_dict, weeks, owner_team_dict, player_id_dict
    )
    save_outputs(
        score_df, sum_df, cumsum_df, standings_df, weekly_points_df, agg_points_df
    )
