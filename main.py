import streamlit as st
import altair as alt
import pandas as pd
import datetime
import asyncio
import aiohttp

async def get_moving_avg(df, **kwargs):
  return df.groupby('city').temperature.rolling(**kwargs).mean().droplevel(0).rolling(**kwargs).mean()

async def get_agg_anomalies(df):
  df['month'] = df.timestamp.dt.month
  grouping = df.groupby(by=['season', 'city']).agg({'temperature': ['mean', 'std']}).temperature.reset_index()
  df = df.merge(grouping, 'left', left_on=['season', 'city'], right_on=['season', 'city'])
  df['anomaly'] = (df['temperature'] < df['mean'] - 2 * df['std']) | (df['temperature'] > df['mean'] + 2 * df['std'])
  return df

async def process_df(df):
  new_df, moving_avg = await asyncio.gather(
      get_agg_anomalies(df),
      get_moving_avg(df, window=30, min_periods=1, center=True)
  )
  new_df['moving_avg'] = moving_avg
  return new_df

async def get_temp_async(city, token, session):
  async with session.get(f'http://api.openweathermap.org/geo/1.0/direct?q={city}&limit=1&appid={token}') as resp:
    coords = (await resp.json())[0]

  async with session.get(
      f'https://api.openweathermap.org/data/2.5/weather?lat={coords["lat"]}&lon={coords["lon"]}&appid={token}',
      params={'units': 'metric'}
  ) as resp:
    return (await resp.json())['main']['temp']

async def is_cur_anomaly_async(city, token, session):
  temp = await get_temp_async(city, token, session)
  city_date_info = df[(df.city==city)&(df.month==datetime.date.today().month)].iloc[0]
  anomaly = not (city_date_info['mean'] - 2 * city_date_info['std'] <= temp <= city_date_info['mean'] + 2 * city_date_info['std'])
  return {'temp': temp, 'anomaly': anomaly}

async def city_tasks(cities, token):
  async with aiohttp.ClientSession() as session:
    tasks = [is_cur_anomaly_async(city, token, session) for city in cities]
    return await asyncio.gather(*tasks)

token = st.text_input("Enter your OpenWeather API key:")
data = st.file_uploader('Load your CSV file:', type=['csv', 'tsv'])

if data:
  df = pd.read_csv(data)
  df.timestamp = pd.to_datetime(df.timestamp)
  df = asyncio.run(process_df(df))

  city = st.selectbox(
    "Which city would you like to analyze?",
    df.city.unique(),
  )

  if city:
    cur = asyncio.run(city_tasks([city], token))[0]

    st.write(
        'Current temperature in {city} is {temp}°C, which is {description} for {month}'.format(
            city=city,
            temp=round(cur['temp'], 1),
            description= '😱AN ANOMALY😱' if cur['anomaly'] else 'normal',
            month=datetime.date.today().strftime('%B')
        )
    )
    
    df_city = df[df.city==city]
    chart = alt.Chart(df_city, title=f"Daily Temperature in {city}")

    hover = alt.selection_point(
        name='hover',
        fields=["timestamp"],
        nearest=True,
        on="mouseover",
        empty="none"
    )

    line_temp = (
        chart
        .mark_line(color='blue')
        .encode(
            x="timestamp",
            y="temperature"
        )
    )

    line_avg = (
        chart
        .mark_line(color='purple')
        .encode(
            x="timestamp",
            y="moving_avg"
        )
    )

    points_temp = chart.mark_point(filled=True, size=5).encode(
        x="timestamp",
        y="temperature",
        color=alt.Color('anomaly', scale=alt.Scale(domain=[True, False], range=["red", 'blue']))
    ).add_params(hover)

    st.altair_chart(line_temp + line_avg + points_temp, use_container_width=True)

    st.dataframe(
      df_city.describe().drop(index='count').temperature
    )

    st.dataframe(
      pd.DataFrame({
        'winter': {'mean': df_city[df_city.season=='winter'].iloc[0]['mean'], 'std': df_city[df_city.season=='winter'].iloc[0]['std']},
        'spring': {'mean': df_city[df_city.season=='spring'].iloc[0]['mean'], 'std': df_city[df_city.season=='spring'].iloc[0]['std']},
        'summer': {'mean': df_city[df_city.season=='summer'].iloc[0]['mean'], 'std': df_city[df_city.season=='summer'].iloc[0]['std']},
        'autumn': {'mean': df_city[df_city.season=='autumn'].iloc[0]['mean'], 'std': df_city[df_city.season=='autumn'].iloc[0]['std']}
      }).T
    )

