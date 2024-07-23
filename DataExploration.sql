Select *
From PortfolioProjectCovid..CovidDeaths$
where continent is not NULL 
order by 3,4;

Select *
From PortfolioProjectCovid..CovidVac
where continent is not NULL
order by 3,4;

-- selecting data we will use

Select location, date, total_cases, new_cases, total_deaths, population
From PortfolioProjectCovid..CovidDeaths$
where continent is not NULL
order by 1,2 

-- looking at total cases vs deaths
-- percentage of deaths from cases by location

Select location, date, total_cases, total_deaths, (CAST(total_deaths as float)/CAST(total_cases as float))*100 as death_percentage
From PortfolioProjectCovid..CovidDeaths$
where continent is not NULL
order by 1,2 

-- looking at total cases/population

Select location, date, total_cases, population, (CAST(total_cases as float)/CAST(population as float))*100 as infection_percentage
From PortfolioProjectCovid..CovidDeaths$
where continent is not NULL
order by 1,2 


-- looking at countries with highest infection rate

Select location, population, MAX(CAST(total_cases AS float)) as peak_cases,
MAX((CAST(total_cases as float)/CAST(population as float)))*100 as infection_percentage
From PortfolioProjectCovid..CovidDeaths$
where continent is not NULL
group by location, population
order by infection_percentage desc;

-- showing countries with highest death count/population

Select location, population, MAX(CAST(total_deaths AS float)) as peak_deaths,
MAX((CAST(total_deaths as float)/CAST(population as float)))*100 as death_percentage
From PortfolioProjectCovid..CovidDeaths$
where continent is not NULL
group by location, population
order by death_percentage desc;

--Breaking down by continent

Select continent, MAX(CAST(total_deaths AS float)) as peak_deaths
From PortfolioProjectCovid..CovidDeaths$
where continent is not NULL
group by continent
order by peak_deaths desc;

--global numbers
-- had to get total cases by week since the new cases are 0 for 6/7 days
SELECT 
    date, 
    SUM(new_cases) AS global_cases, 
    SUM(CAST(new_deaths AS int)) AS global_deaths, 
    CASE 
        WHEN SUM(new_cases) = 0 THEN NULL
        ELSE (SUM(CAST(new_deaths AS int)) / CAST(SUM(new_cases) AS float)) * 100 
    END AS global_death_percentage
FROM 
    PortfolioProjectCovid..CovidDeaths$
WHERE 
    continent IS NOT NULL
GROUP BY 
    date
HAVING 
    CASE 
        WHEN SUM(new_cases) = 0 THEN NULL
        ELSE (SUM(CAST(new_deaths AS int)) / CAST(SUM(new_cases) AS float)) * 100 
    END IS NOT NULL
ORDER BY 
    date, global_cases;

-- global total numbers not grouped by date

SELECT 
    SUM(new_cases) AS global_cases, 
    SUM(CAST(new_deaths AS int)) AS global_deaths,
	SUM(CAST(new_deaths AS int)) / CAST(SUM(new_cases) AS float) * 100  as global_death_percentage
	From PortfolioProjectCovid..CovidDeaths$
	where continent is not null
	order by 1,2 

-- joining vaccine and deaths tables

SELECT dea.continent, dea.location, dea.date, population, vac.new_vaccinations,
SUM(CONVERT(bigint, vac.new_vaccinations)) OVER (Partition by dea.location order by dea.date) as rolling_total_vac
From PortfolioProjectCovid..CovidDeaths$ as dea
join PortfolioProjectCovid..CovidVac as vac
	On dea.location = vac.location
	and dea.date = vac.date
	where dea.continent is not null
	order by 2,3

-- Creating CTE to calculate rolling total

With PopvsVac (continent, location, date, population, new_vaccinations, rolling_total_vac)
as 

(
SELECT dea.continent, dea.location, dea.date, population, vac.new_vaccinations,
SUM(CONVERT(bigint, vac.new_vaccinations)) OVER (Partition by dea.location order by dea.date) as rolling_total_vac
From PortfolioProjectCovid..CovidDeaths$ as dea
join PortfolioProjectCovid..CovidVac as vac
	On dea.location = vac.location
	and dea.date = vac.date
	where dea.continent is not null
	)
Select *, (rolling_total_vac/population)*100 as Percent_rolling
From PopvsVac
order by 2,3 

--temp table method for the same query

DROP Table if exists #PercentPopVac
Create Table #PercentPopVac
(
Continent nvarchar(255),
location nvarchar(255),
date datetime,
population numeric,
new_vaccinations numeric,
rolling_total_vac numeric
)


Insert into #PercentPopVac
SELECT dea.continent, dea.location, dea.date, population, vac.new_vaccinations,
SUM(CONVERT(bigint, vac.new_vaccinations)) OVER (Partition by dea.location order by dea.date) as rolling_total_vac
From PortfolioProjectCovid..CovidDeaths$ as dea
join PortfolioProjectCovid..CovidVac as vac
	On dea.location = vac.location
	and dea.date = vac.date
	where dea.continent is not null
	
Select *, (rolling_total_vac/population)*100 as Percent_rolling
From #PercentPopVac
order by 2,3 

-- creating a view

Create View PercentPopVac as
SELECT dea.continent, dea.location, dea.date, population, vac.new_vaccinations,
SUM(CONVERT(bigint, vac.new_vaccinations)) OVER (Partition by dea.location order by dea.date) as rolling_total_vac
From PortfolioProjectCovid..CovidDeaths$ as dea
join PortfolioProjectCovid..CovidVac as vac
	On dea.location = vac.location
	and dea.date = vac.date
	where dea.continent is not null