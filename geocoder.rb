require 'csv'
require 'google_maps_service'
require 'tty'
require 'pg'
require 'pry'
require 'yaml'

class Geocoder

  PStatementError = Class.new(StandardError)

  def api
    @api ||= GoogleMapsService::Client.new(key: config["google_api_key"], encoding: "utf-8")
  end

  def setup_db
    create_schema
    create_results_table
  end

  def geocode(source_path, source_field_name, string_proc)
    prepare_geocode_results_insert_query
    CSV.foreach(source_path, headers: true) do |row|

      # for some reason, headers are preceded by "\xEF\xBB\xBF" chars in some cases
      headers = row.headers.map {|h| h.sub(/^[^\w]+/,'') }

      i = headers.index(source_field_name)
      raw = row[i]
      next if raw.nil?
      str = string_proc ? string_proc.call(raw) : raw
      results = api.geocode(str)
      results.each {|result| insert_geocode_result(raw, str, Time.now, result.to_json) }
    end
  end

  private

  def insert_geocode_result(raw, str, time, result)
    conn.exec_prepared("insert_geocode_results", [raw, str, time, result])
  end

  def db
    @db ||= YAML.load_file(File.expand_path("./db.yml",File.dirname(__FILE__)))
  end

  def config
    @config ||= YAML.load_file(File.expand_path("./config.yml",File.dirname(__FILE__)))
  end

  def conn
    @conn ||= PG.connect(dbname: db["database"], host: db["host"], port: db["port"], user: db["user"], password: db["password"]).tap do |c|
      c.type_map_for_results = PG::BasicTypeMapForResults.new(c)
      c.type_map_for_queries = PG::BasicTypeMapForQueries.new(c)
    end
  end

  def prompt
    TTY::Prompt.new
  end

  def create_schema
    sql = "CREATE SCHEMA IF NOT EXISTS #{db["schema"]};"
    conn.exec(sql)
  end

  def create_results_table
    sql = <<-SQL
      create table if not exists #{db["schema"]}.results (
        raw_string VARCHAR(256),
        geocoded_string VARCHAR(256),
        time timestamp,
        results JSONB
      );

      CREATE INDEX IF NOT EXISTS idx_results_raw_str ON #{db["schema"]}.results (raw_string);
      CREATE INDEX IF NOT EXISTS idx_results_geocoded_str ON #{db["schema"]}.results (geocoded_string);
      CREATE INDEX IF NOT EXISTS idx_results_results ON #{db["schema"]}.results USING gin (results);
    SQL
    conn.exec(sql)
  end

  def prepare_geocode_results_insert_query
    sql = "INSERT INTO #{db["schema"]}.results ( raw_string, geocoded_string, time, results ) VALUES ( $1::VARCHAR(256), $2::VARCHAR(256), $3::TIMESTAMP, $4::JSONB );"
    begin
      conn.prepare("insert_geocode_results", sql)
    rescue => ex
      raise PStatementError.new(ex.message)
    end
  end

end
